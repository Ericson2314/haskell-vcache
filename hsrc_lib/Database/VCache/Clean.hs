{-# LANGUAGE BangPatterns #-}
-- This module manages the ephemeron tables and VRef caches.
-- 
-- In addition, this thread will signal the writer when there seems
-- to be some GC work to perform.  
--
-- DESIGN THOUGHTS:
--
-- I might be better off removing the ephemeron management into the
-- weak refs, i.e. using the weak ref finalizers. Also, it seems 
-- this cleanup process can become a major performance issue when 
-- run too frequently. 
--
-- A possible performance boost is to, instead of firing a bullet
-- at each cache element, fire a fixed number of bullets based on
-- the cache size and hit rates. This might require splitting the
-- mem_vrefs table in two, such that I know which ones are cached
-- and don't need to worry about shooting into a sparse array.
-- Use of Map.splitRoot may help here.
--
-- Between these two techniques, I might entirely avoid 'sweeping'
-- the cache structures, and avoid interaction with inactive elements.
-- This could lead to big CPU savings when I have a large number of
-- VRefs in memory but they are sparsely populated.
--
module Database.VCache.Clean
    ( cleanStep
    ) where

import Control.Monad
import Control.Concurrent
import Data.Word
import Data.Bits
import Data.Maybe
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.List as L
import Data.IORef
import System.Mem.Weak
import qualified System.Random as Random

import Database.LMDB.Raw
import Database.VCache.Types

type HitRate = Double

-- This step clears GC'd elements from ephemeron tables and manages
-- cache. Currently, this will run about twice per second.
--
-- Clearing an ephemeron table requires holding the vcache_memory
-- MVar. To avoid holding up other users of the MVar, such as VRef
-- allocation, it is important that this operation be incremental.
--
-- Cache management is heuristic, probabilistic, based on exponential
-- decay (i.e. kill X% in each pass). This technique requires minimal
-- information to adapt effectively to varying loads, is unbiased by
-- the order in which VRefs are processed, and is highly incremental.
-- CacheMode and activity give values extra opportunities to survive. 
-- 
cleanStep :: VSpace -> IO ()
cleanStep vc = do
    wtgt <- readIORef (vcache_climit vc)
    w0 <- readIORef (vcache_csize vc)
    clearDeadPVars vc 
    clearDeadVRefs vc 
    signalWriterCleanup vc
    let hrCleanup = -- using heuristic modes (rather than sliding scale)
            if ((100*w0) <  (80*wtgt)) then hrPassiveCleanup else -- 0% to 80%
            if ((100*w0) < (125*wtgt)) then hrBalancedCleanup else -- 80% to 125%
            hrAggressiveCleanup -- more than 125% of target load
    !wf <- cleanVRefsCache vc hrCleanup
    writeIORef (vcache_csize vc) wf
    let bSatisfied = (max w0 wf) < wtgt
    let dtSleep = if bSatisfied then 135000 else 45000
    usleep (10 * dtSleep) -- slowing it down for now; sweep is too slow
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
    stepSize = 800
{-# NOINLINE clearDeadPVars #-}

-- incrementally clear dead VRefs. In case of a VRef shared by many
-- types, some increments may be larger than others... but my expectation
-- is that the amount of sharing is usually small.
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
-- This function processes all the VRefs and returns the total
-- heuristic weight of the remaining cached content.
cleanVRefsCache :: VSpace -> HitRate -> IO Int
cleanVRefsCache vc hfnCleanup = do
    r <- Random.newStdGen
    m <- readMVar (vcache_memory vc)
    let vrephs = L.concatMap Map.elems (Map.elems (mem_vrefs m))
    (CP _ w) <- foldM (cleanVREph hfnCleanup) (CP r 0) vrephs
    return w

data CP = CP !Random.StdGen {-# UNPACK #-} !Int

-- target a single VREph based on the heuristic function. 
cleanVREph :: Double -> CP -> VREph -> IO CP 
cleanVREph !hr cp@(CP r n) (VREph { vreph_cache = wkCache }) =
    deRefWeak wkCache >>= \ mbCache -> case mbCache of
        Nothing -> return cp -- VRef was GC'd at Haskell layer
        Just cache -> atomicModifyIORef cache $ \ c -> case c of
            NotCached -> (NotCached, cp) -- VRef is not currently cached
            Cached v bf -> case xdcln hr r (tickCache bf) of
                (r', Nothing) -> (NotCached, (CP r' n)) -- cache killed
                (r', Just !bf') -> -- cache survived
                    let lgw = 7 + fromIntegral (bf' .&. 0x1f) in
                    let w = 1 `shiftL` lgw in
                    (Cached v bf', CP r' (n + w))

-- 
-- Our Word16 bitfield used by caches has the following format:
--
-- (0x1f)   bits 0..4: heuristic weight of the cached value
-- (0x60)   bits 5..6: cache mode (0..3); policy from programmer
-- (0x80)   bit 7: indicates use; set to 0 upon deref or init
-- (0xf00)  bit 8..11:  use count 
-- (0xf000) bit 12..15: tick count
--
-- The tick count and bit 7 are set to 1 when use count is updated.
-- Use count is updated iff its value is less than the tick count.
-- Consequently, a large use count indicates both many uses and a
-- long history of use.
--

-- Process the tick counter and use counter for the cache bitfield
tickCache :: Word16 -> Word16
tickCache bf | updateUseCount bf = (bf .&. 0x0f7f) + 0x1180
             | maxTickCount bf = bf
             | otherwise = bf + 0x1000
{-# INLINE tickCache #-}

updateUseCount :: Word16 -> Bool
updateUseCount bf = touched && ((tc `shiftR` 4) > uc) where
    touched = (0 == (0x80 .&. bf))
    tc = 0xf000 .&. bf
    uc = 0x0f00 .&. bf
{-# INLINE updateUseCount #-}

maxTickCount :: Word16 -> Bool
maxTickCount bf = (0xf000 .&. bf) == 0xf000
{-# INLINE maxTickCount #-}


-- Compute the per-cycle 'hit rate' based on a target fraction 
-- of kills for a given number of seconds.
--
-- hitRate X N: return D such that, with D probability of 
--  hits per cycle, we'll have X probability of a hit after 
--  N cycles.
--
-- Solving for D:
--
--     (1-X) = (1-D)^N
--     log (1-X) = log ((1-D)^N) = N * log(1-D)
--     D = 1 - exp(log(1-X) % N)
--
-- We'll adjust the number of cycles for the number of hits per kill,
-- and compute cycles based on number of seconds.
killRate :: Double -> Double -> HitRate
killRate kr s = 1 - exp (log (1 - kr) / n) where
    n = s * cycles_per_second / hits_per_kill
    hits_per_kill = 2.0
    cycles_per_second = 2.22

-- A heuristic cleanup function based on exponential decay and game
-- logic. In every cycle, we 'hit' some fraction of cache entries,
-- e.g. with metaphorical bullets. Upon being hit, the cache entry
-- loses one hitpoint. If the entry is reduced to zero hitpoints, 
-- it is killed and removed from the cache. See cacheHit for more.
--  
xdcln :: HitRate -> Random.StdGen -> Word16 -> (Random.StdGen, Maybe Word16)
xdcln !hr !r !bf = 
    let (roll, r') = Random.random r in
    let bf' = if (roll < hr) then cacheHit bf else Just bf in
    (r', bf')
{-# INLINE xdcln #-}

--
-- A cache entry typically starts with 1 hitpoint (CacheMode1) and
-- may gain additional hitpoints when used over a period of time. 
-- Two points on the use counter will grant one extra hitpoint. Each
-- level of CacheMode is worth one hitpoint. 
--
-- In general, we'll assume two hits per kill when deciding cache
-- modes.
--
-- Hitpoints are taken first from the CacheMode. Essentially, this
-- gives CacheMode an amplifying effect: a value with a high mode 
-- has more time to increase its usage count, and every intermediate
-- use will extend this (since CacheMode is reset on every deref).
cacheHit :: Word16 -> Maybe Word16
cacheHit !bf =
    if (0 /= (0x60 .&. bf)) then Just (bf - 0x20) else -- hit absorbed by CacheMode
    if (0 /= (0xe00 .&. bf)) then Just (bf - 0x200) else -- hit absorbed by usage count
    Nothing -- killed

-- Modal hit rates.
hrPassiveCleanup, hrBalancedCleanup, hrAggressiveCleanup :: Double

-- In passive mode, we're at less than 100% load on our cache. We don't
-- need to clean anything up, but it won't hurt to gradually remove the
-- content if idling. This mode aims for a 1% kill rate per minute.
hrPassiveCleanup = killRate 0.01 60

-- In the default mode, we're between 80% and 125% cache load. In this 
-- case, I want about 3% kill rate per second, i.e. such that we can
-- reduce from a 115% load to 100% load in ~five seconds. 
hrBalancedCleanup = killRate 0.03 1

-- In aggressive mode, we've moved up above 125% cache load. At this
-- point, we need to be relatively aggressive in removing content, e.g.
-- killing 9% per second until we're into more reasonable territory.
--
-- If our cache is much larger than this, I'd assume it's a sudden
-- burst of activity or that our cache isn't large enough. But we can
-- take advantage of the natural scaling factor for exponential decay,
-- i.e. at 200% load we'll release about 10% per second.
hrAggressiveCleanup = killRate 0.09 1

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


