

module Database.VCache.VRef
    ( VRef
    , vref, vref'
    , deref, deref'
    , unsafeVRefAddr
    ) where

import System.IO.Unsafe (unsafePerformIO)
import Database.VCache.Types
import Database.VCache.Alloc

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

-- | Dereference a VRef, obtaining its value. If the value is not in
-- cache, it will be read into the database then cached. Otherwise, 
-- the value is read from cache and any expiration timer is reset.
deref :: VRef a -> a
deref = error "TODO: deref"

-- | Dereference a VRef, but do not cache the value. If the value is
-- already cached, the cached value will be used, but the expiration
-- is not reset. If the value is read from the database, its value 
-- will not be cached at all. This can be useful in some contexts,
-- when you know you won't be using the cache. 
--
-- Note: A VRef cache will also be cleared if the VRef itself is GC'd,
-- so often it is sufficient to use dref' just near some root values.
deref' :: VRef a -> a
deref' = error "TODO: deref'"

-- | Each VRef has an numeric address in the VSpace. This address is
-- non-deterministic, and essentially independent of the arguments to
-- the vref constructor. This function is 'unsafe' in the sense that
-- it violates the illusion of purity. However, the VRef address will
-- be stable so long as the developer can guarantee it is reachable.
--
-- This function may be useful for memoization tables and similar.
unsafeVRefAddr :: VRef a -> Address
unsafeVRefAddr = vref_addr

