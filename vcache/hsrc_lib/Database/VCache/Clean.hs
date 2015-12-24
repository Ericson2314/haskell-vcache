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
import qualified Data.List as L
import Data.IORef
import qualified System.Mem.Weak as Weak
import qualified System.Random as Random

import Database.LMDB.Raw
import Database.VCache.Types

-- | Cache cleanup, and signal writer for old content.
cleanStep :: VSpace -> IO ()
cleanStep vc = do
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
    updateCacheSizeEst vc 10 0.01
    wf <- estCacheSize vc

    bsig <- shouldSignalWriter vc
    when bsig (signalWriter vc)

    let bSatisfied = (max w0 wf) < wtgt
    let dtSleep = if bSatisfied then 270000 else 135000
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
    cvrefs <- readMVar (vcache_cvrefs vc)
    return $! Map.size cvrefs

-- sample the cache at random addresses, and update using an
-- exponential running average.
updateCacheSizeEst :: VSpace -> Int -> Double -> IO ()
updateCacheSizeEst vc !n !alpha =
    readMVar (vcache_cvrefs vc) >>= \ cvrefs ->
    if Map.null cvrefs then return () else
    Random.newStdGen >>= \ rgen ->
    let ixs = L.take n $ Random.randomRs (0, Map.size cvrefs - 1) rgen in
    let readAddrSize ix =
            let (_addr, tym) = Map.elemAt ix cvrefs in
            let (_ty, e) = Map.findMin tym in
            readVREphSize e >>= \ esz ->
            return (esz * fromIntegral (Map.size tym))
    in
    mapM readAddrSize ixs >>= \ lSizes ->
    let szTotal = L.foldl' (+) 0 lSizes in
    let sqszTotal = L.foldl' (\ ssq x -> ssq + (x*x)) 0 lSizes in
    let szAvgSamp = fromIntegral (szTotal `div` n) in
    let sqszAvgSamp = fromIntegral (sqszTotal `div` n) in
    readIORef (vcache_csize vc) >>= \ (CacheSizeEst szAvgEst sqszAvgEst) ->
    let upd new old = (alpha * new) + ((1.0 - alpha) * old) in
    let szAvg' = upd szAvgSamp szAvgEst in
    let sqszAvg' = upd sqszAvgSamp sqszAvgEst in
    writeIORef (vcache_csize vc) $! (CacheSizeEst szAvg' sqszAvg')

readVREphSize :: VREph -> IO Int
readVREphSize (VREph { vreph_cache = wk }) =
    Weak.deRefWeak wk >>= \ mbc -> case mbc of
        Nothing -> return 2048 -- GC'd recently; high estimate
        Just cache -> readIORef cache >>= \ c -> case c of
            NotCached ->
                let eMsg = "VCache bug: NotCached element found in vcache_cvrefs" in
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
xclnStrike !vc !r = modifyMVarMasked (vcache_cvrefs vc) $ \ cvrefs ->
    if Map.null cvrefs then return (cvrefs, r) else do
    let (ix,r') = Random.randomR (0, Map.size cvrefs - 1) r
    let (addr, tym) = Map.elemAt ix cvrefs
    tym' <- Map.mapMaybe id <$> TR.traverse strikeVREph tym
    let cvrefs' = if Map.null tym' then Map.delete addr cvrefs
                                   else Map.insert addr tym' cvrefs
    return (cvrefs', r')

-- strikeVREph will reduce the CacheMode for a cached element or
-- remove it from the cache for CacheMode0.
strikeVREph :: VREph -> IO (Maybe VREph)
strikeVREph vreph@(VREph { vreph_cache = wk }) =
    Weak.deRefWeak wk >>= \ mbCache -> case mbCache of
        Nothing -> return Nothing
        Just cache -> atomicModifyIORef cache $ \ c -> case c of
            Cached r bf | (0 /= bf .&. 0x60) ->
                let bf' = (0x80 .|. (bf - 0x20)) in
                let c' = Cached r bf' in
                (c', c' `seq` (Just vreph))
            _ -> (NotCached, Nothing)

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
    let ctEphAddrs = Map.size (mem_vrefs m) + Map.size (mem_pvars m) in
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
