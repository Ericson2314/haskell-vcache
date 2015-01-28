

module Database.VCache.VRef
    ( VRef
    , vref, vref'
    , deref, deref'
    , unsafeVRefAddr
    ) where

import Control.Monad
import Data.Bits
import Data.IORef
import System.IO.Unsafe 
import Database.VCache.Types
import Database.VCache.Alloc
import Database.VCache.Read

-- | Construct a VRef for a cacheable value. This will search for a
-- matching value on disk (for structure sharing) and otherwise will
-- arrange for the value to be written to disk. 
vref :: (VCacheable a) => VSpace -> a -> VRef a
vref vc v = unsafePerformIO (newVRefIO vc v CacheMode1)

-- | The normal vref constructor will cache the value, using heuristic
-- timeouts and weight metrics to decide when to remove the value from
-- cache. Each subsequent deref will extend the timeout. 
--
-- While this behavior is useful in many cases (especially when updating
-- B-trees or map-like data structures), there are cases where developers
-- know they will not deref the value in the near future, or where they
-- cannot afford to hold onto large values and must shunt them to disk 
-- ASAP. In these cases, the vref' constructor or deref' accessor allow
-- developers to bypass the caching behavior.
--
vref' :: (VCacheable a) => VSpace -> a -> VRef a
vref' vc v = unsafePerformIO (newVRefIO' vc v)

readVRef :: VRef a -> IO (a, Int)
readVRef v = readAddrIO (vref_space v) (vref_addr v) (vref_parse v)
{-# INLINE readVRef #-}

-- | Dereference a VRef, obtaining its value. If the value is not in
-- cache, it will be read into the database then cached. Otherwise, 
-- the value is read from cache and any expiration timer is reset.
--
-- Assuming a valid VCacheable instance, this operation should return
-- an equivalent value as used to construct the VRef.
deref :: VRef a -> a
deref v = unsafePerformIO $
    unsafeInterleaveIO (readVRef v) >>= \ lazy_read_rw ->
    atomicModifyIORef (vref_cache v) $ \ c -> case c of
        Cached r w -> 
            let c' = Cached r (w .&. 0x7f) in 
            c' `seq` (c',r)
        NotCached ->
            let (r,w) = lazy_read_rw in
            let c' = mkVRefCache r w CacheMode1 in
            c' `seq` (c',r)

-- | Dereference a VRef. Will use the cached value if available, but
-- will not cache the value or reset any expiration timers. This can
-- be useful when processing large values, since it can limit how long
-- those values remain in memory.
deref' :: VRef a -> a
deref' v = unsafePerformIO $ 
    readIORef (vref_cache v) >>= \ c -> case c of
        Cached r _ -> return r
        NotCached -> liftM fst (readVRef v)

-- | Each VRef has an numeric address in the VSpace. This address is
-- non-deterministic, and essentially independent of the arguments to
-- the vref constructor. This function is 'unsafe' in the sense that
-- it violates the illusion of purity. However, the VRef address will
-- be stable so long as the developer can guarantee it is reachable.
--
-- This function may be useful for memoization tables and similar.
unsafeVRefAddr :: VRef a -> Address
unsafeVRefAddr = vref_addr

