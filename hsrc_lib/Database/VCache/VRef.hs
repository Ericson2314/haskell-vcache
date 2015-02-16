

module Database.VCache.VRef
    ( VRef
    , vref, deref
    , vref', deref'
    , unsafeVRefAddr
    , unsafeVRefRefct
    , vref_space
    , CacheMode(..)
    , vrefc, derefc
    ) where

import Control.Monad
import Data.IORef
import System.IO.Unsafe 
import Database.VCache.Types
import Database.VCache.Alloc
import Database.VCache.Read

-- | Construct a reference with the cache initially active, i.e.
-- such that immediate deref can access the value without reading
-- from the database. The given value will be placed in the cache
-- unless the same vref has already been constructed.
vref :: (VCacheable a) => VSpace -> a -> VRef a
vref = vrefc CacheMode1 
{-# INLINE vref #-}

-- | Construct a VRef with an alternative cache control mode. 
vrefc :: (VCacheable a) => CacheMode -> VSpace -> a -> VRef a
vrefc cm vc v = unsafePerformIO (newVRefIO vc v cm)
{-# INLINABLE vrefc #-}

-- | In some cases, developers can reasonably assume they won't need a 
-- value in the near future. In these cases, use the vref' constructor
-- to allocate a VRef without caching the content. 
vref' :: (VCacheable a) => VSpace -> a -> VRef a
vref' vc v = unsafePerformIO (newVRefIO' vc v)
{-# INLINABLE vref' #-}

readVRef :: VRef a -> IO (a, Int)
readVRef v = readAddrIO (vref_space v) (vref_addr v) (vref_parse v)
{-# INLINE readVRef #-}

-- | Dereference a VRef, obtaining its value. If the value is not in
-- cache, it will be read into the database then cached. Otherwise, 
-- the value is read from cache and the cache is touched to restart
-- any expiration.
--
-- Assuming a valid VCacheable instance, this operation should return
-- an equivalent value as was used to construct the VRef.
deref :: VRef a -> a
deref = derefc CacheMode1
{-# INLINE deref #-}

-- | Dereference a VRef with an alternative cache control mode.
derefc :: CacheMode -> VRef a -> a
derefc cm v = unsafeDupablePerformIO $ 
    unsafeInterleaveIO (readVRef v) >>= \ lazy_read_rw ->
    join $ atomicModifyIORef (vref_cache v) $ \ c -> case c of
        Cached r bf ->
            let bf' = touchCache cm bf in
            let c' = Cached r bf' in
            (c', c' `seq` return r)
        NotCached ->
            let (r,w) = lazy_read_rw in
            let c' = mkVRefCache r w cm in
            let op = initVRefCache v >> return r in
            (c', c' `seq` op)
{-# NOINLINE derefc #-}


-- I've modified how VRefs are recorded 


-- | Dereference a VRef. This will read from the cache if the value
-- is available, but will not update the cache. If the value is not
-- cached, it will be read instead from the persistence layer.
--
-- This can be useful if you know you'll only dereference a value 
-- once for a given task, or if the datatype involved is cheap to
-- parse (e.g. simple bytestrings) such that there isn't a strong
-- need to cache the parse result.
deref' :: VRef a -> a
deref' v = unsafePerformIO $ 
    readIORef (vref_cache v) >>= \ c -> case c of
        Cached r _ -> return r
        NotCached -> liftM fst (readVRef v)
{-# INLINABLE deref' #-}

-- | Each VRef has an numeric address in the VSpace. This address is
-- non-deterministic, and essentially independent of the arguments to
-- the vref constructor. This function is 'unsafe' in the sense that
-- it violates the illusion of purity. However, the VRef address will
-- be stable so long as the developer can guarantee it is reachable.
--
-- This function may be useful for memoization tables and similar.
--
-- The 'Show' instance for VRef will also show the address.
unsafeVRefAddr :: VRef a -> Address
unsafeVRefAddr = vref_addr
{-# INLINE unsafeVRefAddr #-}

-- | This function allows developers to access the reference count 
-- for the VRef that is currently recorded in the database. This may
-- be useful for heuristic purposes. However, caveats are needed:
--
-- First, due to structure sharing, a VRef may share an address with
-- VRefs of other types having the same serialized form. Reference 
-- counts are at the address level.
--
-- Second, because the VCache writer operates in a background thread,
-- the reference count returned here may be slightly out of date.
--
-- Third, it is possible that VCache will eventually use some other
-- form of garbage collection than reference counting. This function
-- should be considered an unstable element of the API.
unsafeVRefRefct :: VRef a -> IO Int
unsafeVRefRefct v = readRefctIO (vref_space v) (vref_addr v) 
{-# INLINE unsafeVRefRefct #-}



