

module Database.VCache.FromAddr 
    ( addr2vref
    , addr2pvar
    , addr2pvar_new
    ) where

import Control.Monad
import Data.IORef
import Data.Typeable
import qualified Data.Map.Strict as Map
import Control.Concurrent.STM.TVar
import System.Mem.Weak (Weak)
import qualified System.Mem.Weak as Weak
import System.IO.Unsafe
import Unsafe.Coerce

-- import Database.LMDB.Raw
import Database.VCache.Types
import Database.VCache.Read
-- import Database.VCache.RWLock

-- | Obtain a VRef given an address and value. The given address will
-- not be cached initially.
addr2vref :: (VCacheable a) => VSpace -> Address -> Cache a -> IO (VRef a)
addr2vref space addr ini = 
    if not (isVRefAddr addr) then fail ("invalid VRef address " ++ show addr) else
    loadMemCache undefined space addr ini >>= \ cache ->
    return $! VRef 
        { vref_addr  = addr
        , vref_cache = cache
        , vref_space = space
        , vref_parse = get
        }
{-# INLINABLE addr2vref #-}

-- | Load or Create the cache for a given location and type.
loadMemCache :: (Typeable a) => a -> VSpace -> Address -> Cache a -> IO (IORef (Cache a))
loadMemCache _dummy space addr ini = atomicModifyIORef mcrf loadCache where
    mcrf = vcache_mem_vrefs space
    typa = typeOf _dummy
    getCache = unsafeDupablePerformIO . Weak.deRefWeak . _unsafeEphWeak
    find mc = Map.lookup addr mc >>= Map.lookup typa >>= getCache
    loadCache mc = case find mc of
        Just c -> (mc, c)
        Nothing -> unsafePerformIO $ do
            c <- newIORef ini
            wc <- mkWeakIORef c (return ())
            let eph = Eph { eph_addr = addr, eph_type = typa, eph_cache = wc }
            let mc' = addEph eph mc
            return (mc', c)
{-# NOINLINE loadMemCache #-}

addEph :: Eph -> EphMap -> EphMap
addEph e = Map.alter (Just . maybe i0 ins) (eph_addr e) where
    ty = eph_type e
    i0 = Map.singleton ty e
    ins = Map.insert ty e
{-# INLINABLE addEph #-}

-- this is a bit of a hack... it is unsafe in general, since
-- Eph doesn't track the type `a`. We'll coerce it, assuming
-- the type is known in context.
_unsafeEphWeak :: Eph -> Weak (IORef (Cache a))
_unsafeEphWeak (Eph { eph_cache = w }) = _unsafeCoerceWeakCache w 
{-# INLINE _unsafeEphWeak #-}

-- unsafe coercion; used in contexts where we know type 
-- (via matching TypeRep and location); similar to Data.Dynamic.
_unsafeCoerceWeakCache :: Weak (IORef (Cache b)) -> Weak (IORef (Cache a))
_unsafeCoerceWeakCache = unsafeCoerce
{-# INLINE _unsafeCoerceWeakCache #-}

-- | Obtain a PVar given an address. The PVar will lazily load
-- when first read, and only if read.
addr2pvar :: (VCacheable a) => VSpace -> Address -> IO (PVar a)
addr2pvar space addr = rdLazy >>= addr2pvar_ini space addr where
    rdLazy = unsafeInterleaveIO rdStrict
    rdStrict = liftM (RDV . fst) (readAddrIO space addr get)
{-# INLINE addr2pvar #-}

-- | Given an address for a PVar and an initial value, return the
-- PVar. The initial value will be dropped if the PVar has already
-- been loaded with a value.
addr2pvar_new :: (VCacheable a) => VSpace -> Address -> a -> IO (PVar a)
addr2pvar_new space addr = addr2pvar_ini space addr . RDV
{-# INLINE addr2pvar_new #-}

addr2pvar_ini :: (VCacheable a) => VSpace -> Address -> RDV a -> IO (PVar a)
addr2pvar_ini space addr ini =
    if not (isPVarAddr addr) then fail ("invalid PVar address " ++ show addr) else
    loadPVarData undefined space addr ini >>= \ pvdata ->
    return $! PVar
        { pvar_addr = addr
        , pvar_data = pvdata
        , pvar_space = space
        , pvar_write = put
        }
{-# INLINABLE addr2pvar_ini #-}

loadPVarData :: (Typeable a) => a -> VSpace -> Address -> RDV a -> IO (TVar (RDV a))
loadPVarData _dummy space addr ini = atomicModifyIORef pvtbl loadData >>= id where
    pvtbl = vcache_mem_pvars space
    typa = typeOf _dummy
    getData = unsafeDupablePerformIO . Weak.deRefWeak . _unsafeDataWeak
    loadData mpv = case Map.lookup addr mpv of
        Nothing -> initData mpv
        Just e ->
            let bTypeMismatch = pveph_type e /= typa in
            if bTypeMismatch then (mpv, fail (eTypeMismatch e)) else
            case getData e of
                Nothing -> initData mpv
                Just d  -> (mpv, return d)
    initData mpv = unsafePerformIO $ do
        d <- newTVarIO ini
        wd <- mkWeakTVar d (return ())
        let pve = PVEph { pveph_addr = addr, pveph_type = typa, pveph_data = wd }
        let mpv' = addPVEph pve mpv 
        return (mpv', return d)
    eTypeMismatch e = ($ "") $
        showString "PVar user error: address " . shows addr .
        showString " type mismatch on load. " .
        showString " Existing: " . shows (pveph_type e) .
        showString " Expecting: " . shows typa
{-# NOINLINE loadPVarData #-}

addPVEph :: PVEph -> PVEphMap -> PVEphMap
addPVEph pve = Map.insert (pveph_addr pve) pve
{-# INLINE addPVEph #-}

_unsafeDataWeak :: PVEph -> Weak (TVar (RDV a))
_unsafeDataWeak (PVEph { pveph_data = w }) = _unsafeCoerceWeakData w
{-# INLINE _unsafeDataWeak #-}

_unsafeCoerceWeakData :: Weak (TVar (RDV b)) -> Weak (TVar (RDV a))
_unsafeCoerceWeakData = unsafeCoerce
{-# INLINE _unsafeCoerceWeakData #-}













