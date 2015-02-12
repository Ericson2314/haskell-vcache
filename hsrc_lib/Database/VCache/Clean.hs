{-# LANGUAGE BangPatterns #-}
-- This module manages the ephemeron tables and VRef caches.
-- 
-- In addition, this thread will signal the writer when there seems
-- to be some GC work to perform.  
module Database.VCache.Clean
    ( cleanStep
    ) where

import Control.Monad
import Control.Exception
import Control.Concurrent
import Data.Word
import Data.Bits
import Data.Maybe
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.List as L
import Data.IORef
import System.Mem.Weak

import Database.LMDB.Raw
import Database.VCache.Types

-- heuristic cleanup function; return True to delete.
type HFnCleanup = Word16 -> Maybe Word16


-- In each step, we must clear ephemeron tables and manage cache.
--
-- One challenge here is that we mustn't hold the vcache_memory lock
-- for too long, otherwise we'll hold up threads performing allocations
-- or other tasks. Mostly, this is an issue when dealing with tens of
-- thousands of VRefs and PVars; I can work with large batches so long
-- as they're predictably sized.
--
-- The other major challenge is that we must decide which VRefs to
-- clear. I don't keep enough information to use precise algorithms.
-- Simple heuristics, i.e. based on timeouts and usage tracking, may
-- end up deleting the whole cache or not enough of it. I think that
-- a probabilistic approach should be viable, i.e. clearing some 
-- fraction of cached elements in each round but adjusting somehow
-- for frequency and recency of use.
--
-- If everything seems clean, we'll slow down a little and let the
-- mutators catch up to create a new mess. :)
-- 
cleanStep :: VSpace -> IO ()
cleanStep vc = do
    wtgt <- readIORef (vcache_climit vc)
    w0 <- readIORef (vcache_csize vc)

    clearDeadPVars vc 
    clearDeadVRefs vc 
    signalWriterCleanup vc

    let hfnCleanup = -- adjust cleanup heuristic based on last round 
            if ((4*w0) < (3*wtgt)) then hfnPassiveCleanup else -- < 75%
            if ((4*w0) < (5*wtgt)) then hfnDefaultCleanup else -- < 125%
            hfnAggressiveCleanup -- >= 125%
    !wf <- cleanVRefsCache vc hfnCleanup
    writeIORef (vcache_csize vc) wf

    let bSatisfied = (wf < wtgt) && (w0 < wtgt)
    let dtSleep = if bSatisfied then 135000 else 45000
    usleep dtSleep 
{-# NOINLINE cleanStep #-}

-- sleep for a number of microseconds
usleep :: Int -> IO ()
usleep = threadDelay
{-# INLINE usleep #-}
-- Aside: I've had some bad experiences with threadDelay in a loop
-- causing space leaks. Supposedly, this has been fixed. But if not
-- this might be the culprit.

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
    stepSize = 1000
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
    stepSize = 700
{-# NOINLINE clearDeadVRefs #-}

-- If the writer has some work it could be doing, signal it. This
-- won't significantly affect a busy writer, but an idle one needs
-- the occasional kick in the pants to finish up GC tasks or clear
-- the recent allocations table. (We want to clear the allocations
-- table because it may hold onto PVars or VRefs via alloc_deps.)
--
signalWriterCleanup :: VSpace -> IO ()
signalWriterCleanup vc = do
    m <- readMVar (vcache_memory vc)
    ctZeroes <- readZeroesCt vc
    let bHoldingAllocs = not (emptyAllocation (mem_alloc m))
    let ctEphAddrs = Map.size (mem_vrefs m) + Map.size (mem_pvars m)
    let bNeedGC = ctZeroes > ctEphAddrs
    let bWrite = bNeedGC || bHoldingAllocs
    when bWrite (signalWriter vc)
{-# NOINLINE signalWriterCleanup #-}

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



-- The heuristic cleanup function has most of the cleanup logic.
-- This function will process all the VRefs and compute the total
-- heuristic weight of the remaining cached content.
cleanVRefsCache :: VSpace -> HFnCleanup -> IO Int
cleanVRefsCache vc hfnCleanup = do
    m <- readMVar (vcache_memory vc)
    let vrephs = L.concatMap Map.elems (Map.elems (mem_vrefs m))
    foldM (cleanVREph hfnCleanup) 0 vrephs

-- clear a single VREph based on the heuristic function, and also
-- accumulate the total heuristic weight of the cached content. 
cleanVREph :: HFnCleanup -> Int -> VREph -> IO Int
cleanVREph hfnCleanup !n (VREph { vreph_cache = wkCache }) =
    deRefWeak wkCache >>= \ mbCache -> case mbCache of
        Nothing -> return n -- VRef was GC'd at Haskell layer
        Just cache -> atomicModifyIORef cache $ \ c -> case c of
            NotCached -> (NotCached, n) -- VRef is not currently cached
            Cached v bf -> case gcCache hfnCleanup bf of
                Nothing -> (NotCached, n) -- VRef cache cleared
                Just !bf' -> -- keeping cached content
                    let lgw = 7 + fromIntegral (bf' .&. 0x1f) in
                    let w = 1 `shiftL` lgw in
                    (Cached v bf', n + w)


-- TODO: continue tuning this model

-- 
-- Our Word16 bitfield used by caches has the following format:
--
-- (0x1f)   bits 0..4: heuristic weight of the cached value
-- (0x60)   bits 5..6: cache mode (0..3); policy from programmer
-- (0x80)   bit 7: cleared on each deref, set to 1 by GC
-- (0x700)  bit 8..10: reuse tracker (0..7)
--    0x400 for high bit
--    0x100 for low bit
-- (0xf800) bit 11..15: gcTouch counter (1..31 after initial touch)
--    0x8000 for high bit
--    0x0800 for low bit
--
-- Rather than a full reset of the timer, I've decided to cut it in
-- half on each use, thus preserving some information about relative
-- frequency of use. 
--
-- The reuse tracker then increases whenever the touch counter is
-- large. High reuse numbers thus indicate both that we've used the
-- cached value many times and that there have been some large gaps 
-- between uses, so reuse tracker is a good indicator of whether we
-- should keep a value.
--
-- For the moment, the cache mode policy will be implemented as a 
-- sort of 'extra life', i.e. anything selected for destruction may
-- survive but reducing the cache mode.
-- 

-- touchCache handles the basic updates. If the cache was recently
-- used, we'll reduce the touch count and update the reuse tracker,
-- and we'll never delete a recently used object. 
--
-- Otherwise, we will increment the touch count and pass it on to 
-- the heuristic decision for cleanup.
gcCache :: HFnCleanup -> HFnCleanup
gcCache _ bf | (0 == (0x80 .&. bf)) = Just $! -- cache was touched by user.
    if (0 == (0xff00 .&. bf)) then 0x880 .|. bf else -- newly cached.
    let tc' = (0x400 + (bf `shiftR` 1)) .&. 0xf800 in -- floor((tc+1)/2)
    let rt = 0x700 .&. bf in
    let bUpdRT = (0x700 /= rt) && (tc' >= (rt `shiftL` 3)) in
    let rt' = if bUpdRT then (rt + 0x100) else rt in
    (0x80 .|. tc' .|. rt' .|. (bf .&. 0x7f))
gcCache hfnCleanup bf | (0xf800 == (0xf800 .&. bf)) = hfnCleanup bf
gcCache hfnCleanup bf = hfnCleanup (bf + 0x800)
    
-- During passive cleanup, we don't need to delete anything. I could
-- possibly clear stuff anyway, but I'll go ahead and leave it alone.
-- Consequently, our passive-mode cache manager is only doing the 
-- basic touchCache and counting bytes.
hfnPassiveCleanup :: HFnCleanup
hfnPassiveCleanup = Just
{-# INLINABLE hfnPassiveCleanup #-}

-- we'll gradually destroy values, favoring those with low priority
-- and that haven't seen much use. 
hfnDefaultCleanup :: HFnCleanup
hfnDefaultCleanup = hfnBasicCleanup 8 4

-- similar to default, but more aggressive. 
hfnAggressiveCleanup :: HFnCleanup
hfnAggressiveCleanup = hfnBasicCleanup 4 2

-- The idea here is that we can treat reuse trackers and cache modes
-- as 'extra lives' for keeping a value in memory. Every time a VRef
-- is dereferenced, it resets the cache mode, reduces timeout, and 
-- potentially adds to the reuse tracker, thus keeping the value in
-- memory for another few fractions of a second.
--
-- If this mode is sustained, eventually all unused values will be
-- timed out and removed. However, we'll typically switch to
-- a less aggressive mode
hfnBasicCleanup :: Word16 -> Word16 -> HFnCleanup
hfnBasicCleanup !t0 !backoff !bf =
    assert (t0 >= backoff) $ 
    let tc = (0xf800 .&. bf) `shiftR` 11 in -- 1..31
    let rt = (0x700 .&. bf) `shiftR` 8 in -- 0..7
    let cm = (0x60 .&. bf) `shiftR` 5 in -- 0..3
    let h  = t0 + rt + cm in -- minimal timeout
    if (tc <= h) then Just bf else
    let bf' = bf - (backoff `shiftL` 11) in
    killCache bf'
{-# INLINABLE hfnBasicCleanup #-}

-- Clean an item, but with extra lives from reuse tracker or cache
-- mode. CacheModes are each valued at one full 'extra life'. Reuse 
-- counts are valued at half that.
-- 
-- The idea here is that, by preserving an object in the cache and
-- backing off a little, we'll be able to GC enough unused values 
-- that we can return to a less aggressive collection mode.
killCache :: HFnCleanup
killCache !bf =
    if (0 /= (bf .&. 0x600)) then Just (bf - 0x200) else -- take from reuse counter
    if (0 /= (bf .&. 0x60)) then Just (bf - 0x20) else -- take from CacheMode
    Nothing -- cache killed

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


