
-- primary implementation of VCache (might break up later)
module Database.VCache.Impl 
    ( addr2vref
    , loadMemCache
    ) where

import Data.IORef
import Data.Map.Strict (Map)
import Data.Word
import Data.Typeable
import Data.Typeable.Internal (TypeRep(..),Fingerprint(..))
import qualified Data.Map.Strict as M
import Database.VCache.Types

import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.List as L

import System.Mem.Weak (Weak)
import qualified System.Mem.Weak as Weak
import System.IO.Unsafe
import Unsafe.Coerce

-- | Obtain a VRef given an address 
addr2vref :: (VCacheable a) => VSpace -> Address -> IO (VRef a)
addr2vref space addr = 
    loadMemCache undefined space addr >>= \ cache ->
    return $! VRef 
        { vref_addr  = addr
        , vref_cache = cache
        , vref_space = space
        , vref_parse = get
        }
{-# INLINE addr2vref #-}

-- a few small prime numbers for hashing
p_100, p_1000, p_10k :: Word64
p_100 = 541
p_1000 = 7919
p_10k = 104729

-- | Load or Create the cache for a given location and type.
--
-- Other than the GC operation, this is the only function that should 
-- touch vcache_mem_vrefs. It is both a reader and a writer.
loadMemCache :: (Typeable a) => a -> VSpace -> Address -> IO (IORef (Cache a))
loadMemCache _dummy space addr = atomicModifyIORef mcrf loadCache where
    mcrf = vcache_mem_vrefs space
    typa = typeOf _dummy
    hkey = fromIntegral $ _hty typa + (addr * p_1000)
    match eph = (addr == eph_addr eph) -- must match address
             && (typa == eph_type eph) -- must match type
    getCache = unsafeDupablePerformIO . Weak.deRefWeak . _unsafeEphWeak
    loadCache mc =
        let oldCache = IntMap.lookup hkey mc >>= L.find match >>= getCache in
        case oldCache of
            Just c -> (mc, c)
            Nothing -> unsafePerformIO (initCache mc)
    initCache mc = do
        c <- newIORef NotCached
        wc <- mkWeakIORef c (return ())
        let eph = Eph { eph_addr = addr, eph_type = typa, eph_weak = wc }
        let addEph = Just . (eph:) . maybe [] id
        let mc' = IntMap.alter addEph hkey mc 
        return (mc',c)

-- this is a bit of a hack... it is unsafe in general, since
-- Eph doesn't track the type `a`. We'll coerce it, assuming
-- the type is known in context.
_unsafeEphWeak :: Eph -> Weak (IORef (Cache a))
_unsafeEphWeak (Eph { eph_weak = w }) = _unsafeCoerceWeakCache w 
{-# INLINE _unsafeEphWeak #-}

-- unsafe coercion; used in contexts where we know type 
-- (via matching TypeRep and location); similar to Data.Dynamic.
_unsafeCoerceWeakCache :: Weak (IORef (Cache b)) -> Weak (IORef (Cache a))
_unsafeCoerceWeakCache = unsafeCoerce
{-# INLINE _unsafeCoerceWeakCache #-}

-- simple hash for a typerep. I'm going to assume the hash is
-- good enough already, so I'm just combining the parts.
_hty :: TypeRep -> Word64
_hty (TypeRep (Fingerprint a b) _ _) = (a * p_10k) + (b * p_100)


