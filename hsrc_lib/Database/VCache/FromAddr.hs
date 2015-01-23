

module Database.VCache.FromAddr 
    ( addr2vref
    , addr2pvar
    , addr2pvar_new
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

-- | Obtain a VRef given an address. Begins in a not-cached state.
addr2vref :: (VCacheable a) => VSpace -> Address -> IO (VRef a)
addr2vref space addr = _addr2vref_ini space addr NotCached
{-# INLINE addr2vref #-}

-- | Obtain a VRef given an address and value. The given value will
-- be used as the initial cache contents, but only if the cache does
-- not already exist. If the cache exists, the existing cache will
-- be shared using whatever state it already has.
_addr2vref_ini :: (VCacheable a) => VSpace -> Address -> Cache a -> IO (VRef a)
_addr2vref_ini space addr ini = 
    if not (isVRefAddr addr) then fail ("invalid VRef address " ++ show addr) else
    loadMemCache undefined space addr ini >>= \ cache ->
    return $! VRef 
        { vref_addr  = addr
        , vref_cache = cache
        , vref_space = space
        , vref_parse = get
        }
{-# INLINABLE _addr2vref_ini #-}

-- | Load or Create the cache for a given location and type.
loadMemCache :: (Typeable a) => a -> VSpace -> Address -> Cache a -> IO (IORef (Cache a))
loadMemCache _dummy space addr ini = atomicModifyIORef mcrf loadCache where
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
        c <- newIORef ini
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


-- | Obtain a PVar given an address. The PVar will lazily load
-- when first read.
addr2pvar :: (VCacheable a) => VSpace -> Address -> IO (PVar a)
addr2pvar space addr = _addr2pvar_ini space addr (Left get)
{-# INLINE addr2pvar #-}

-- | Given an address for a *new* PVar, with the initial value.
-- This is the same as addr2pvar, but will load the PVar with the
-- given initial value rather than setting it to lazily load on
-- first read. 
--
-- This operation is unsafe (you can lose information) if you don't
-- guarantee that this is the first use of the given PVar address. 
addr2pvar_new :: (VCacheable a) => VSpace -> Address -> a -> IO (PVar a)
addr2pvar_new space addr val = _addr2pvar_ini space addr (Right val)
{-# INLINE addr2pvar_new #-}

_addr2pvar_ini :: (VCacheable a) => VSpace -> Address -> RDV a -> IO (PVar a)
_addr2pvar_ini space addr ini =
    if not (isPVarAddr addr) then fail ("invalid PVar address " ++ show addr) else
    loadPVarData undefined space addr ini >>= \ pvdata ->
    return $! PVar
        { pvar_addr = addr
        , pvar_data = pvdata
        , pvar_space = space
        , pvar_write = put
        }
{-# INLINABLE _addr2pvar_ini #-}

loadPVarData :: (Typeable a) => a -> VSpace -> Address -> RDV a -> IO (TVar (RDV a))
loadPVarData _dummy space addr ini = atomicModifyIORef pvtbl loadData where
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
        d <- newTVarIO ini
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













