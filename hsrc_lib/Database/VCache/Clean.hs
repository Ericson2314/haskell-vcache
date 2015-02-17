{-# LANGUAGE BangPatterns #-}
-- This module manages the ephemeron tables and VRef caches.
-- 
-- In addition, this thread will signal the writer when there seems
-- to be some GC work to perform.  
--
-- DESIGN THOUGHTS:
--
-- The original implementation was inefficient, touching far too many
-- VRefs in each pass. It didn't scale nicely.
--
-- I've redesigned to partition VRefs so I can easily find just those
-- that are cached. And the cleanup function now touches only those in
-- cache. Clearing GC'd content is now handled by the System.Mem.Weak 
-- finalizers. Size estimates must be probabilistic, to avoid a global
-- pass to compute sizes.
--
-- At this point, the clean function only touches elements that are 
-- certainly cached, and which it plans to remove from cache. The 
-- cleanup function is based on exponential decay, i.e. we try to
-- remove X% of the cache in each round. Though X may vary based on
-- whether we are over or under our heuristic cache limit.
--
-- Originally, I had some sophisticated usage tracking. I could move
-- some of this to the touchCache operation, but for now I'm just
-- going to assume that CacheMode alone is sufficient in most cases
-- due to how it resets on each deref.
--
module Database.VCache.Clean
    ( cleanStep
    ) where

import Control.Monad
import Control.Applicative
import Control.Concurrent
import Data.Bits
import qualified Data.Traversable as TR
import qualified Data.Map.Strict as Map
import Data.IORef
import qualified System.Mem.Weak as Weak
import qualified System.Random as Random

import Database.LMDB.Raw
import Database.VCache.Types

-- | Cache cleanup, and signal writer for old content.
cleanStep :: VSpace -> IO ()
cleanStep vc = do
    bsig <- shouldSignalWriter vc
    when bsig (signalWriter vc)

    wtgt <- readIORef (vcache_climit vc)
    w0 <- estCacheSize vc
    let hitRate = 
            if ((100 * w0) < ( 80 * wtgt)) then 0.00 else
            if ((100 * w0) < (100 * wtgt)) then 0.01 else
            if ((100 * w0) < (120 * wtgt)) then 0.02 else
            if ((100 * w0) < (150 * wtgt)) then 0.03 else
            if ((100 * w0) < (190 * wtgt)) then 0.04 else 
            if ((100 * w0) < (240 * wtgt)) then 0.05 else
            0.06
    xcln vc hitRate
    updateCacheSizeEst vc
    wf <- estCacheSize vc

    let bSatisfied = (max w0 wf) < wtgt
    let dtSleep = if bSatisfied then 295000 else 95000 
    usleep dtSleep -- ~10Hz, slower when steady

-- sleep for a number of microseconds
usleep :: Int -> IO ()
usleep = threadDelay
{-# INLINE usleep #-}

-- For now, I'm choosing to use sqrt( avgSquare ) because it is 
-- weighted in favor of larger values, which (conversely) we're
-- less likely to find when randomly sampling a collection with 
-- just a few large values and lots of small ones. 
--
estCacheSize :: VSpace -> IO Int
estCacheSize vc = do
    csze <- readIORef (vcache_csize vc)
    let avgAddr = sqrt (csze_addr_sqsz csze) 
    ctAddrs <- fromIntegral <$> readCacheAddrCt vc
    return $! ceiling $ avgAddr * ctAddrs

readCacheAddrCt :: VSpace -> IO Int
readCacheAddrCt vc = do
    m <- readMVar (vcache_memory vc)
    return $! Map.size (mem_cvrefs m)

-- sample the cache at a few random addresses, use this to update the
-- cache size by a small factor. Over the course of many seconds, the
-- estimated average size per address should approach the actual size
-- assuming the average itself is stable. Even if average size isn't
-- stable, this is good enough to help guide the cache manager.
--
-- The assumption here is that the cvrefs map is usually large. If it
-- is small, we'll still use the same algorithm, even if it's a bit 
-- redundant, to simplify reasoning and testing. A constant number of
-- samples are taken in each round. Probabilistically
updateCacheSizeEst :: VSpace -> IO ()
updateCacheSizeEst vc =
    readMVar (vcache_memory vc) >>= \ m ->
    let cvrefs = mem_cvrefs m in
    if Map.null cvrefs then return () else
    let nextIx = Random.randomR (0, Map.size cvrefs - 1) in
    let loop !n !r !sz !sqsz = 
            if (0 == n) then return (sz,sqsz) else
            let (ix,r') = nextIx r in
            let (_, tym) = Map.elemAt ix cvrefs in
            let (_, e) = Map.findMin tym in -- safe; address elements non-empty
            readVREphSize e >>= \ esz ->
            let addrsz = fromIntegral $ esz * Map.size tym in
            let sz' = sz + addrsz in
            let sqsz' = sqsz + (addrsz * addrsz) in
            loop (n-1) r' sz' sqsz'
    in
    let nSamples = 15 :: Int in
    Random.newStdGen >>= \ r ->
    loop nSamples r 0 0 >>= \ (totalSize, totalSqSize) ->
    let sampleAvg = totalSize / fromIntegral nSamples in
    let sampleAvgSq = totalSqSize / fromIntegral nSamples in
    readIORef (vcache_csize vc) >>= \ (CacheSizeEst oldAvg oldAvgSq) ->
    let alpha = 0.015 :: Double in
    let newAvg = alpha * sampleAvg + ((1.0 - alpha) * oldAvg) in
    let newAvgSq = alpha * sampleAvgSq + ((1.0 - alpha) * oldAvgSq) in
    writeIORef (vcache_csize vc) $! (CacheSizeEst newAvg newAvgSq)

readVREphSize :: VREph -> IO Int
readVREphSize (VREph { vreph_cache = wk }) =
    Weak.deRefWeak wk >>= \ mbc -> case mbc of 
        Nothing -> return 2048 -- GC'd recently; high estimate
        Just cache -> readIORef cache >>= \ c -> case c of
            NotCached -> 
                let eMsg = "VCache bug: NotCached element found in mem_cvrefs" in
                fail eMsg
            Cached _ bf ->
                let lgSz = 6 + fromIntegral (0x1f .&. bf) in
                return $! 1 `shiftL` lgSz

-- | exponential decay based cleanup. In this case, we attack a
-- random fraction of the cached addresses. Each attack reduces
-- the CacheMode of cached elements. If the CacheMode is zero, the
-- element is removed from the database. Active contents have their
-- CacheMode reset on each use, and cleanup stops when estimated 
-- size is good. 
xcln :: VSpace -> Double -> IO ()
xcln !vc !hr = do
    ct <- readCacheAddrCt vc
    let hct = ceiling $ hr * fromIntegral ct 
    r <- Random.newStdGen
    xclnLoop vc hct r

xclnLoop :: VSpace -> Int -> Random.StdGen -> IO ()
xclnLoop !vc !n !r =
    if (n < 1) then return () else
    xclnStrike vc r >>= xclnLoop vc (n-1)

xclnStrike :: VSpace -> Random.StdGen -> IO Random.StdGen
xclnStrike !vc !r = modifyMVarMasked (vcache_memory vc) $ \ m ->
    if Map.null (mem_cvrefs m) then return (m,r) else do
    let cvrefs = mem_cvrefs m
    let evrefs = mem_evrefs m
    let (ix,r') = Random.randomR (0, Map.size cvrefs - 1) r
    let (addr, tym) = Map.elemAt ix cvrefs 
    (tymc, tyme) <- Map.mapEither id <$> TR.traverse strikeVREph tym
    let cvrefs' = if Map.null tymc then Map.delete addr cvrefs else
                  if Map.null tyme then cvrefs else
                  Map.insert addr tymc cvrefs
    let evrefs' = if Map.null tyme then evrefs else
                  Map.insertWith (Map.union) addr tyme evrefs
    let m' = m { mem_cvrefs = cvrefs', mem_evrefs = evrefs' }
    return (m', m' `seq` r')

-- strikeVREph will reduce the CacheMode for a cached element or
-- remove it from the cache (in right) for CacheMode0.
strikeVREph :: VREph -> IO (Either VREph VREph)
strikeVREph vreph@(VREph { vreph_cache = wk }) =
    Weak.deRefWeak wk >>= \ mbCache -> case mbCache of
        Nothing -> return (Right vreph) -- 
        Just cache -> atomicModifyIORef cache $ \ c -> case c of
            Cached r bf | (0 /= bf .&. 0x60) -> 
                let bf' = (0x80 .|. (bf - 0x20)) in
                let c' = Cached r bf' in
                (c', c' `seq` (Left vreph))
            _ -> (NotCached, Right vreph)

-- If the writer has obvious work it could be doing, signal it. This
-- won't significantly affect a busy writer, but an idle writer may
-- require a kick in the pants to remove content from the allocations
-- list or clear old zeroes.
shouldSignalWriter :: VSpace -> IO Bool
shouldSignalWriter vc = 
    readMVar (vcache_memory vc) >>= \ m ->
    let bHoldingAllocs = not (emptyAllocation (mem_alloc m)) in
    if bHoldingAllocs then return True else
    readZeroesCt vc >>= \ ctZeroes ->
    let ctEphAddrs = Map.size (mem_cvrefs m) + Map.size (mem_evrefs m) + Map.size (mem_pvars m) in
    if (ctEphAddrs < ctZeroes) then return True else
    return False

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
