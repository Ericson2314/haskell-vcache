
-- This module manages the ephemeron tables and VRef caches.
-- 
-- In addition, this thread will signal the writer when there seems
-- to be some GC work to perform.  
module Database.VCache.Clean
    ( cleanStep
    ) where

import Control.Monad
import Control.Concurrent
import Data.Maybe
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.List as L
import System.Mem.Weak

import Database.LMDB.Raw
import Database.VCache.Types

-- In each step, we must clear ephemeron tables and manage garbage.
--
-- One challenge here is that we mustn't hold the vcache_memory lock
-- for too long, otherwise we'll hold up threads performing allocations
-- or other tasks. Mostly, this is an issue when dealing with tens of
-- thousands of VRefs and PVars; I can work with fairly large batches
-- so long as they're predictably sized.
--
-- The other major challenge is that we must decide which VRefs to
-- clear. At the moment, I'm not keeping any sophisticated records
-- of cache state; I have only the previous cache size. 
-- 
cleanStep :: VSpace -> IO ()
cleanStep vc = do
    clearDeadPVars vc 
    clearDeadVRefs vc 
    cleanVRefsCache vc
    signalWriterCleanup vc
    usleep 45000 -- ~20-25 Hz
{-# NOINLINE cleanStep #-}

-- incrementally clear dead PVars.
clearDeadPVars :: VSpace -> IO ()
clearDeadPVars vc = runStepper step where
    step k = 
        modifyMVarMasked (vcache_memory vc) $ \ m ->
        incrementalMapMaybeM stepSize upd k (mem_pvars m) >>= \ (k', pvars') ->
        let m' = m { mem_pvars = pvars' } in
        m' `seq` return (m', k')
    upd _ pve = 
        testEph pve >>= \ bAlive ->
        return $ if bAlive then Just pve else Nothing
    testEph (PVEph { pveph_data = wk }) = liftM isJust $ deRefWeak wk
    stepSize = 800
{-# NOINLINE clearDeadPVars #-}

-- incrementally clear dead VRefs.
clearDeadVRefs :: VSpace -> IO ()
clearDeadVRefs vc = runStepper step where
    step k = 
        modifyMVarMasked (vcache_memory vc) $ \ m ->
        incrementalMapMaybeM stepSize upd k (mem_vrefs m) >>= \ (k', vrefs') ->
        let m' = m { mem_vrefs = vrefs' } in
        m' `seq` return (m', k')
    upd _ tyMap =
        filterM (testEph . snd) (Map.toAscList tyMap) >>= \ xs ->
        if (L.null xs) then return Nothing else
        return (Just (Map.fromAscList xs))
    testEph (VREph { vreph_cache = wk }) = liftM isJust $ deRefWeak wk
    stepSize = 600
{-# NOINLINE clearDeadVRefs #-}

-- if the writer is idling but has some work it could do, signal it.
-- (if not idling, signal it too, but it won't make much difference.)
--
-- This heuristic isn't perfect, but it does take care of the startup
-- time cleanups.
signalWriterCleanup :: VSpace -> IO ()
signalWriterCleanup vc = do
    m <- readMVar (vcache_memory vc)
    ctZeroes <- readZeroesCt vc
    let bHoldingAllocs = not (emptyAllocation (mem_alloc m))
    let ctEphAddrs = Map.size (mem_vrefs m) + Map.size (mem_pvars m)
    let bNeedGC = ctZeroes > ctEphAddrs
    let bWrite = bNeedGC || bHoldingAllocs
    when bWrite (signalWriter vc)

readZeroesCt :: VSpace -> IO Int
readZeroesCt vc = withRdOnlyTxn vc $ \ txn ->
    mdb_stat' txn (vcache_db_refct0 vc) >>= \ stat ->
    return $! fromIntegral (ms_entries stat)

emptyAllocation :: Allocator -> Bool
emptyAllocation ac = fn n && fn c && fn p where
    n = alloc_frm_next ac
    c = alloc_frm_curr ac
    p = alloc_frm_prev ac
    fn = Map.null . alloc_list

signalWriter :: VSpace -> IO ()
signalWriter vc = void $ tryPutMVar (vcache_signal vc) ()

-- sleep for a number of microseconds
usleep :: Int -> IO ()
usleep = threadDelay
{-# INLINE usleep #-}
-- Aside: I've had some bad experiences with threadDelay in a loop
-- causing space leaks. Supposedly, this has been fixed. But if not
-- this might be the culprit.

-- We also need to make a sweep through the VRefs to clear cached
-- values. 
cleanVRefsCache :: VSpace -> IO ()
cleanVRefsCache vc =
    -- putStrLn "TODO: cleanVRefsCache"
    -- TODO
    return ()




-- incrementally update a map, processing at most nKeys at a time then
-- return the last key processed.
incrementalMapMaybeM :: (Monad m, Ord k) => Int -> (k -> a -> m (Maybe a)) -> Maybe k -> Map k a -> m (Maybe k, Map k a)
incrementalMapMaybeM nKeys update k0 m0 = 
    if (nKeys < 1) then return (k0,m0) else -- can't do anything
    let mk0 = maybe m0 (snd . flip Map.split m0) k0 in -- map m0 to right of k0
    let bDone = nKeys >= Map.size mk0 in -- is this the last step?
    let fnKeys = if bDone then id else L.take nKeys in
    let lKV = fnKeys (Map.toAscList mk0) in
    let k' = if bDone then Nothing else Just $! fst $! L.last lKV in
    let step m (k,v) =
            update k v >>= \ mbv' ->
            return $! maybe (Map.delete k) (Map.insert k) mbv' m
    in
    k' `seq`
    foldM step m0 lKV >>= \ m' ->
    return (k', m')
{-# INLINABLE incrementalMapMaybeM #-}

-- start with Nothing, end with Nothing...
runStepper :: (Monad m) => (Maybe a -> m (Maybe a)) -> m ()
runStepper step = begin >>= continue where
    begin = step Nothing
    continue Nothing = return ()
    continue k = step k >>= continue
{-# INLINABLE runStepper #-}


