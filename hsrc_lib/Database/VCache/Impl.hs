
-- primary implementation of VCache (might break up later)
module Database.VCache.Impl 
    ( addr2vref, addr2pvar
    ) where

import Data.IORef
-- import Data.Word
import Data.Typeable
import Data.Typeable.Internal (TypeRep(..),Fingerprint(..))
-- import Data.Map.Strict (Map)
-- import qualified Data.Map.Strict as M
-- import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.List as L
import Control.Concurrent.STM.TVar
import System.Mem.Weak (Weak)
import qualified System.Mem.Weak as Weak
import System.IO.Unsafe
import Unsafe.Coerce

-- import Database.LMDB.Raw
import Database.VCache.Types
-- import Database.VCache.RWLock

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

-- | Load or Create the cache for a given location and type.
--
-- Other than the GC operation, this is the only function that should 
-- touch vcache_mem_vrefs. It is both a reader and a writer.
loadMemCache :: (Typeable a) => a -> VSpace -> Address -> IO (IORef (Cache a))
loadMemCache _dummy space addr = atomicModifyIORef mcrf loadCache where
    mcrf = vcache_mem_vrefs space
    typa = typeOf _dummy
    hkey = hashVRef typa addr
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
{-# NOINLINE loadMemCache #-}

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


-- Hash function for the VRef ephemeron table
--
-- In this case, I want to include the type representation
-- because address collisions are quite possible for many
-- different types.
--
-- By comparison, PVars don't use typerep for hashing;
-- multiple typereps for one address is illegal anyway.
hashVRef :: TypeRep -> Address -> Int
hashVRef (TypeRep (Fingerprint a b) _ _) addr = hA + hB + hAddr where
    hA = p_10k * fromIntegral a 
    hB = p_100 * fromIntegral b
    hAddr = p_1000 * fromIntegral addr
    p_100 = 541
    p_1000 = 7919
    p_10k = 104729
{-# INLINE hashVRef #-}


-- | Obtain a PVar given an address
addr2pvar :: (VCacheable a) => VSpace -> Address -> IO (PVar a)
addr2pvar space addr =
    loadPVarData undefined space addr >>= \ pvdata ->
    return $! PVar
        { pvar_addr = addr
        , pvar_data = pvdata
        , pvar_space = space
        , pvar_write = put
        }
{-# INLINE addr2pvar #-}

loadPVarData :: (VCacheable a) => a -> VSpace -> Address -> IO (TVar (RDV a))
loadPVarData _dummy space addr = atomicModifyIORef pvtbl loadData where
    pvtbl = vcache_mem_pvars space
    typa = typeOf _dummy
    hkey = fromIntegral addr
    match eph = (addr == pveph_addr eph)
    getData = unsafeDupablePerformIO . Weak.deRefWeak . _unsafeDataWeak
    loadData mpv = case IntMap.lookup hkey mpv >>= L.find match of
        Just e ->
            if (pveph_type e == typa) then tryEph mpv e else
            error "PVar error: multiple types for single address" 
        Nothing -> newData mpv
    tryEph mpv eph = case getData eph of
        Just d -> (mpv, d)
        Nothing -> newData mpv
    newData = unsafePerformIO . initData
    initData mpv = do
        d <- newTVarIO (Left get)
        wd <- mkWeakTVar d (return ())
        let eph = PVEph { pveph_addr = addr, pveph_type = typa, pveph_weak = wd }
        let addEph = Just . (eph:) . maybe [] id
        let mpv' = IntMap.alter addEph hkey mpv
        return (mpv', d)
{-# NOINLINE loadPVarData #-}

_unsafeDataWeak :: PVEph -> Weak (TVar (RDV a))
_unsafeDataWeak (PVEph { pveph_weak = w }) = _unsafeCoerceWeakData w
{-# INLINE _unsafeDataWeak #-}

_unsafeCoerceWeakData :: Weak (TVar (RDV b)) -> Weak (TVar (RDV a))
_unsafeCoerceWeakData = unsafeCoerce
{-# INLINE _unsafeCoerceWeakData #-}
















