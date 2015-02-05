

module Database.VCache.FromAddr 
    ( addr2vref
    , addr2pvar
    , addr2pvar_new
    ) where

import Control.Monad
import Control.Exception
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

-- | Obtain a VRef given an address and value. Not initially cached.
--
-- This operation will return Nothing if the requested address has
-- been selected for garbage collection.
addr2vref :: (VCacheable a) => VSpace -> Address -> IO (Maybe (VRef a))
addr2vref vc addr = assert (isVRefAddr addr) $
    loadMemCache undefined vc addr >>= \ mbCache -> case mbCache of 
        Nothing -> return Nothing
        Just cache -> return (Just (VRef addr cache vc get))
{-# INLINABLE addr2vref #-}

-- | Test whether an address has been recently GC'd. This is mostly
-- for VRefs, since we need to arbitrate potential revival of VRefs
-- for structure sharing.
recentGC :: Collector -> Address -> Bool
recentGC c addr = Map.member addr (c_gcf_curr c) 
               || Map.member addr (c_gcf_prev c) 
{-# INLINE recentGC #-}

-- | Load or Create the cache for a given location and type.
-- Or return Nothing if the address has been selected for GC. 
loadMemCache :: (Typeable a) => a -> VSpace -> Address -> IO (Maybe (IORef (Cache a)))
loadMemCache _dummy vc addr = 
    let ty = typeOf _dummy in
    atomicModifyIORef (vcache_collector vc) $ \ c ->
        if recentGC c addr then (c, Nothing) else
        let em = c_mem_vrefs c in
        let mbf = Map.lookup addr em >>= Map.lookup ty >>= _getCache in
        case mbf of
            r@(Just _) -> (c,r)
            Nothing -> unsafePerformIO $ do
                d <- newIORef NotCached
                wd <- mkWeakIORef d (return ())
                let e = Eph addr ty wd
                let c' = c { c_mem_vrefs = addEph e em }
                return (c', Just d)
{-# NOINLINE loadMemCache #-}

addEph :: Eph -> EphMap -> EphMap
addEph e = Map.alter (Just . maybe i0 ins) (eph_addr e) where
    ty = eph_type e
    i0 = Map.singleton ty e
    ins = Map.insert ty e
{-# INLINABLE addEph #-}

_getCache :: Eph -> Maybe (IORef (Cache a))
_getCache = unsafeDupablePerformIO . Weak.deRefWeak . _unsafeEphWeak
{-# INLINE _getCache #-}

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
addr2pvar vc addr = rdLazy >>= addr2pvar_ini vc addr where
    rdLazy = unsafeInterleaveIO rdStrict
    rdStrict = liftM (RDV . fst) (readAddrIO vc addr get)
{-# INLINE addr2pvar #-}

-- | Given an address for a PVar and an initial value, return the
-- PVar. The initial value will be dropped if the PVar has already
-- been loaded with a value.
addr2pvar_new :: (VCacheable a) => VSpace -> Address -> a -> IO (PVar a)
addr2pvar_new vc addr = addr2pvar_ini vc addr . RDV
{-# INLINE addr2pvar_new #-}

addr2pvar_ini :: (VCacheable a) => VSpace -> Address -> RDV a -> IO (PVar a)
addr2pvar_ini vc addr ini = assert (isPVarAddr addr) $ 
    loadPVarData undefined vc addr ini >>= \ pvdata ->
    return $! PVar
        { pvar_addr = addr
        , pvar_data = pvdata
        , pvar_space = vc
        , pvar_write = put
        }
{-# INLINABLE addr2pvar_ini #-}

-- | Obtain the TVar associated with a PVar, or fail on type mismatch.
loadPVarData :: (Typeable a) => a -> VSpace -> Address -> RDV a -> IO (TVar (RDV a))
loadPVarData _dummy vc addr ini = 
    let ty = typeOf _dummy in
    join $ atomicModifyIORef (vcache_collector vc) $ \ c ->
        assert (not (recentGC c addr)) $ -- revival of PVar? Not possible.
        let mpv = c_mem_pvars c in
        let create = unsafePerformIO $ do
                d <- newTVarIO ini
                wd <- mkWeakTVar d (return ())
                let e = PVEph addr ty wd 
                let c' = c { c_mem_pvars = addPVEph e mpv }
                return (c', return d)
        in
        case Map.lookup addr mpv of
            Nothing -> create
            Just e ->
                let tye = pveph_type e in
                let eMsg = eTypeMismatch addr ty tye in
                if ty /= tye then (c, fail eMsg) else
                case _getData e of
                    Nothing -> create
                    Just d -> (c, return d)
{-# NOINLINE loadPVarData #-}

eTypeMismatch :: Address -> TypeRep -> TypeRep -> String
eTypeMismatch addr tyNew tyOld = 
    showString "PVar user error: address " . shows addr .
    showString " type mismatch on load. " .
    showString " Existing: " . shows tyOld .
    showString " Expecting: " . shows tyNew $ ""

addPVEph :: PVEph -> PVEphMap -> PVEphMap
addPVEph pve = Map.insert (pveph_addr pve) pve
{-# INLINE addPVEph #-}

_getData :: PVEph -> Maybe (TVar (RDV a))
_getData = unsafeDupablePerformIO . Weak.deRefWeak . _unsafeDataWeak
{-# INLINE _getData #-}

_unsafeDataWeak :: PVEph -> Weak (TVar (RDV a))
_unsafeDataWeak (PVEph { pveph_data = w }) = _unsafeCoerceWeakData w
{-# INLINE _unsafeDataWeak #-}

_unsafeCoerceWeakData :: Weak (TVar (RDV b)) -> Weak (TVar (RDV a))
_unsafeCoerceWeakData = unsafeCoerce
{-# INLINE _unsafeCoerceWeakData #-}


