
-- primary implementation of VCache (might break up later)
module Database.VCache.Impl 
    ( mvref
    , addr2vref
    , loadMemCache
    ) where

import Control.Concurrent.MVar
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



-- | Move a VRef into a target destination space. This will exit fast
-- if the VRef is already in the target location, otherwise performs a
-- deep copy. 
--
-- In general, developers should try to avoid moving values between 
-- caches. This is more efficient than parsing and serializing values,
-- but is still expensive and shouldn't happen by accident. Developers
-- using more than one VCache (a rather unusual scenario) should have
-- a pretty good idea about the dataflow between them to minimize this
-- operation.
--
mvref :: VSpace -> VRef a -> VRef a
mvref sp ref =
    if (sp == vref_space ref) then ref else
    _deepCopy sp ref
{-# INLINE mvref #-}

_deepCopy :: VSpace -> VRef a -> VRef a
_deepCopy dst src = error "todo: mvref deep copy"

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

-- | Load or Create the cache for a given space, type, address triple.
--
-- This uses unsafePerformIO under the hood to support the infinite cache
-- illusion. Garbage collection managed by a separate thread.
loadMemCache :: (Typeable a) => a -> VSpace -> Address -> IO (MVar (Cached a))
loadMemCache _dummy space addr = atomicModifyIORef mcrf loadCache where
    mcrf = vcache_mem_vrefs space
    typa = typeOf _dummy
    hkey = fromIntegral $ _hty typa + (addr * 4231)
    match eph = (addr == eph_addr eph) -- must match address
             && (typa == eph_type eph) -- must match type
    getCache = unsafeDupablePerformIO . Weak.deRefWeak . _ephWeak
    loadCache mc =
        let oldCache = IntMap.lookup hkey mc >>= L.find match >>= getCache in
        case oldCache of
            Just c -> (mc, c)
            Nothing -> unsafePerformIO (initCache mc)
    initCache mc = do
        c <- newEmptyMVar
        wc <- mkWeakMVar c (return ())
        let eph = Eph { eph_addr = addr, eph_type = typa, eph_weak = wc }
        let addEph = Just . maybe [eph] (eph:)
        let mc' = IntMap.alter addEph hkey mc 
        return (mc',c)

-- this is a bit of a hack... it is unsafe in general, but we know it is safe
-- for loadMemCache because we've already matched on address and typerep.
_ephWeak :: Eph -> Weak (MVar (Cached a))
_ephWeak (Eph { eph_weak = w }) = _cw w 

_cw :: Weak (MVar (Cached b)) -> Weak (MVar (Cached a))
_cw = unsafeCoerce

-- simple hash for a typerep.
_hty :: TypeRep -> Word64
_hty (TypeRep (Fingerprint a b) _ _) = (a * 6679) + b
    

