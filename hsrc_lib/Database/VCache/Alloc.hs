{-# LANGUAGE ForeignFunctionInterface, BangPatterns #-}

-- | Constructors and Allocators for VRefs and PVars.
--
-- This module is the nexus of many concurrency concerns. Addresses
-- are GC'd, but structure sharing allows VRef addresses to revive
-- from zero references. Allocations might not be observed in the
-- LMDB layer databases for a couple frames. GC at the Haskell layer
-- must interact with ephemeron maps.
--
-- Because VRefs are frequently constructed by unsafePerformIO, and
-- PVars may be constructed via unsafePerformIO (newPVarIO), none of
-- these operations may use STM.
--
-- For now, I'm essentially using a global lock for managing these
-- in-memory tables. Since the LMDB layer is also limited by a single
-- writer, I think this lock shouldn't become a major bottleneck. But
-- it may require more context switching than desirable.
-- 
module Database.VCache.Alloc
    ( addr2vref
    , addr2pvar
    , newVRefIO
    , newVRefIO'
    , initVRefCache
    , newPVar
    , newPVars
    , newPVarIO
    , newPVarsIO
    , loadRootPVar
    , loadRootPVarIO
    ) where

import Control.Exception
import Control.Monad
import Control.Applicative
import Data.Word
import Data.IORef
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.List as L
import qualified Data.Map.Strict as Map
import Data.Typeable
import Data.Maybe

import Foreign.C.Types
import Foreign.Ptr
import Foreign.Storable
import Foreign.Marshal.Alloc

import GHC.Conc (unsafeIOToSTM)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar 

import System.Mem.Weak (Weak)
import qualified System.Mem.Weak as Weak
import System.IO.Unsafe
import Unsafe.Coerce

import Database.LMDB.Raw

import Database.VCache.Types
import Database.VCache.Path
import Database.VCache.Aligned
import Database.VCache.VPutFini
import Database.VCache.Hash
import Database.VCache.Read

-- | Obtain a VRef given an address and value. Not initially cached.
-- This operation doesn't touch the persistence layer; it assumes the
-- given address is valid.
--
addr2vref :: (VCacheable a) => VSpace -> Address -> IO (VRef a)
addr2vref !vc !addr = 
    assert (isVRefAddr addr) $ 
    modifyMVarMasked (vcache_memory vc) $ 
    addr2vref' vc addr
{-# INLINE addr2vref #-}

addr2vref' :: (VCacheable a) => VSpace -> Address -> Memory -> IO (Memory, VRef a)
addr2vref' = _addr2vref undefined 
{-# INLINE addr2vref' #-}

-- Since I'm partitioning VRefs based on whether they're cached, I 
-- must search two maps to see whether the VRef is already in memory. 
_addr2vref :: (VCacheable a) => a -> VSpace -> Address -> Memory -> IO (Memory, VRef a)
_addr2vref _dummy !vc !addr !m = do
    let ty = typeOf _dummy 
    mbCache <- loadVRefCache addr ty (mem_vrefs m)
    case mbCache of
        Just cache -> return (m, VRef addr cache vc ty get)
        Nothing -> do
            cache <- newIORef NotCached
            e <- mkVREph vc addr cache ty
            let m' = m { mem_vrefs = addVREph e (mem_vrefs m) }
            m' `seq` return (m', VRef addr cache vc ty get)

mkVREph :: VSpace -> Address -> IORef (Cache a) -> TypeRep -> IO VREph
mkVREph !vc !addr !cache !ty = 
    mkWeakIORef cache (clearVRef vc addr ty) >>= \ wkCache ->
    return $! VREph addr ty wkCache
{-# INLINE mkVREph #-}

loadVRefCache :: Address -> TypeRep -> VREphMap -> IO (Maybe (IORef (Cache a)))
loadVRefCache !addr !ty !em = mbrun _getVREphCache mbf where
    mbf = Map.lookup addr em >>= Map.lookup ty
{-# INLINE loadVRefCache #-}

-- When a VRef is GC'd from the Haskell layer, it must be deleted from
-- the ephemeron table. And deleting it from the cache will also help
-- the cache manager maintain a valid estimate of cache size.  
--
-- There is no guarantee that this operation is timely. It may be called
-- after the address is brought back into memory. So this function will
-- double check that it's still working with a 'dead' ephemeron.
clearVRef :: VSpace -> Address -> TypeRep -> IO ()
clearVRef !vc !addr !ty = delFromCache >> delFromMem where
    delFromCache = modifyMVarMasked_ (vcache_cvrefs vc) delFrom
    delFromMem = modifyMVarMasked_ (vcache_memory vc) $ \ m ->
        delFrom (mem_vrefs m) >>= \ vrefs' ->
        return $! m { mem_vrefs = vrefs' }
    delFrom em = case takeVREph addr ty em of
        Nothing -> return em
        Just (VREph { vreph_cache = wk }, em') ->
            Weak.deRefWeak wk >>= \ mbc ->
            if isJust mbc then return em  -- replaced since GC; do not delete
                          else return em' -- removed

-- This is certainly an unsafe operation in general, but we have
-- already validated the TypeRep matches.
_getVREphCache :: VREph -> IO (Maybe (IORef (Cache a)))
_getVREphCache = Weak.deRefWeak . _u where
    _u :: VREph -> Weak (IORef (Cache a))
    _u (VREph { vreph_cache = w }) = _c w 
    _c :: Weak (IORef (Cache b)) -> Weak (IORef (Cache a))
    _c = unsafeCoerce
{-# INLINE _getVREphCache #-}

mbrun :: (Applicative m) => (a -> m (Maybe b)) -> Maybe a -> m (Maybe b)
mbrun = maybe (pure Nothing) 
{-# INLINE mbrun #-}
 
-- | Obtain a PVar given an address. PVar will lazily load when read,
-- but otherwise remains in memory until released by the programmer.
-- If the PVar is already in memory, the underlying TVar will be shared.
-- (Or this operation will fail if the TypeRep is not identical.)
addr2pvar :: (VCacheable a) => VSpace -> Address -> IO (PVar a)
addr2pvar !vc !addr = 
    assert (isPVarAddr addr) $ 
    modifyMVarMasked (vcache_memory vc) $ 
    addr2pvar' vc addr
{-# INLINE addr2pvar #-}

addr2pvar' :: (VCacheable a) => VSpace -> Address -> Memory -> IO (Memory, PVar a)
addr2pvar' = _addr2pvar undefined
{-# INLINE addr2pvar' #-}

_addr2pvar :: (VCacheable a) => a -> VSpace -> Address -> Memory -> IO (Memory, PVar a)
_addr2pvar _dummy !vc !addr m = do
    let ty = typeOf _dummy
    mbVar <- loadPVarTVar addr ty (mem_pvars m)
    case mbVar of
        Just var -> return (m, PVar addr var vc ty put)
        Nothing -> do
            lzv <- unsafeInterleaveIO $ RDV . fst <$> readAddrIO vc addr get
            var <- newTVarIO lzv
            e <- mkPVEph vc addr var ty
            let m' = m { mem_pvars = addPVEph e (mem_pvars m) }
            m' `seq` return (m', PVar addr var vc ty put)


mkPVEph :: VSpace -> Address -> TVar (RDV a) -> TypeRep -> IO PVEph
mkPVEph !vc !addr !tvar !ty = 
    mkWeakTVar tvar (clearPVar vc addr) >>= \ wkTVar ->
    return $! PVEph addr ty wkTVar
{-# INLINE mkPVEph #-}

loadPVarTVar :: Address -> TypeRep -> PVEphMap -> IO (Maybe (TVar (RDV a)))
loadPVarTVar !addr !ty !mpv =
    case Map.lookup addr mpv of
        Nothing -> return Nothing
        Just pve -> do
            let tye = pveph_type pve 
            unless (ty == tye) (fail $ eTypeMismatch addr ty tye)
            _getPVEphTVar pve

eTypeMismatch :: Address -> TypeRep -> TypeRep -> String
eTypeMismatch addr tyNew tyOld = 
    showString "PVar user error: address " . shows addr .
    showString " type mismatch on load. " .
    showString " Existing: " . shows tyOld .
    showString " Expecting: " . shows tyNew $ ""

-- Clear a PVar from the ephemeron map.
clearPVar :: VSpace -> Address -> IO ()
clearPVar !vc !addr = modifyMVarMasked_ (vcache_memory vc) $ \ m -> do
    pvars' <- tryDelPVEph addr (mem_pvars m)
    let m' = m { mem_pvars = pvars' }
    return $! m'

tryDelPVEph :: Address -> PVEphMap -> IO PVEphMap
tryDelPVEph !addr !mpv =
    case Map.lookup addr mpv of
        Nothing -> return mpv
        Just (PVEph { pveph_data = wk }) ->
            Weak.deRefWeak wk >>= \ mbd ->
            if isJust mbd then return mpv else
            return $! Map.delete addr mpv

-- unsafe: get data, assuming that type already matches.
_getPVEphTVar :: PVEph -> IO (Maybe (TVar (RDV a)))
_getPVEphTVar = Weak.deRefWeak . _u where
    _u :: PVEph -> Weak (TVar (RDV a))
    _u (PVEph { pveph_data = w }) = _c w
    _c :: Weak (TVar (RDV b)) -> Weak (TVar (RDV a))
    _c = unsafeCoerce
{-# INLINE _getPVEphTVar #-}

-- | Construct a new VRef and initialize cache with given value.
-- If cache exists, will touch existing cache as if dereferenced.
newVRefIO :: (VCacheable a) => VSpace -> a -> CacheMode -> IO (VRef a)
newVRefIO vc v cm = 
    runVPutIO vc (put v) >>= \ ((), _data) ->
    allocVRefIO vc _data >>= \ vref ->
    join $ atomicModifyIORef (vref_cache vref) $ \ c -> case c of
        Cached r bf ->
            let bf' = touchCache cm bf in
            let c' = Cached r bf' in
            (c', c' `seq` return vref)
        NotCached ->
            let c' = mkVRefCache v (BS.length _data) cm in
            let op = initVRefCache vref >> return vref in
            (c', c' `seq` op)
{-# NOINLINE newVRefIO #-}

-- Add VREph to the vcache_cvrefs table for cache management.
--
-- I have an option here, to either create a new weak IORef for the
-- cache or to reuse the existing one. I'm choosing the latter because
-- I imagine ephemerons to have a fair amount of overhead.
initVRefCache :: VRef a -> IO ()
initVRefCache !r = do
    let vc = vref_space r 
    vrefs <- mem_vrefs <$> readMVar (vcache_memory vc)
    case Map.lookup (vref_addr r) vrefs >>= Map.lookup (vref_type r) of
        Nothing -> fail $ "VCache bug: " ++ show r ++ " should be in mem_vrefs!"
        Just e -> modifyMVarMasked_ (vcache_cvrefs vc) $ return . addVREph e
{-# INLINABLE initVRefCache #-}

-- | Construct a new VRef without touching the cache. This allows the
-- value to be removed from memory soon after construction.
newVRefIO' :: (VCacheable a) => VSpace -> a -> IO (VRef a) 
newVRefIO' !vc v = runVPutIO vc (put v) >>= allocVRefIO vc . snd
{-# INLINE newVRefIO' #-}

-- | Allocate a VRef given its data. First, we'll try to find a match
-- in the database. If there is none, we'll look for a match among the
-- recent allocations. Only if no match is found will we prepare a new
-- allocation.
--
allocVRefIO :: (VCacheable a) => VSpace -> ByteString -> IO (VRef a)
allocVRefIO !vc !_data = 
    let _name = hash _data in
    withByteStringVal _name $ \ vName ->
    withByteStringVal _data $ \ vData ->
    withRdOnlyTxn vc $ \ txn ->
    listDups' txn (vcache_db_caddrs vc) vName >>= \ caddrs ->
    seek (candidateVRefInDB vc txn vData) caddrs >>= \ mbVRef ->
    case mbVRef of
        Just !vref -> return vref -- found matching VRef in database
        Nothing -> modifyMVarMasked (vcache_memory vc) $ \ m -> 
            case findRecentVRef _name _data m of
                Just addr -> addr2vref' vc addr m
                Nothing -> do -- allocate new vref
                    let ac = mem_alloc m
                    let addr = alloc_new_addr ac
                    let frm' = addVRefAlloc addr _name _data (alloc_frm_next ac)
                    let addr' = 2 + (alloc_new_addr ac) 
                    let ac' = ac { alloc_new_addr = addr', alloc_frm_next = frm' }
                    let m' = m { mem_alloc = ac' }
                    signalAlloc vc
                    addr2vref' vc addr m'

-- add new VRef to next allocation frame.
addVRefAlloc :: Address -> ByteString -> ByteString -> AllocFrame -> AllocFrame
addVRefAlloc addr _name _data (AllocFrame l s r i) =
    let ul = Map.insert addr _data in
    let insAddr = Just . (:) addr . fromMaybe [] in
    let us = Map.alter insAddr _name in
    AllocFrame (ul l) (us s) r i

-- Find recently allocated VRef with a matching data, if one exists.
-- This will also filter out addresses that are recently GC'd.
findRecentVRef :: ByteString -> ByteString -> Memory -> Maybe Address
findRecentVRef _name _data m = allocFrameSearch ff (mem_alloc m) where
    ff frm = Map.lookup _name (alloc_seek frm) >>= L.find (match frm)
    match frm addr = isVRef && dataMatch && available where
        isVRef = isVRefAddr addr
        dataMatch = maybe False (== _data) (Map.lookup addr (alloc_list frm))
        available = not $ recentGC (mem_gc m) addr

-- list all values associated with a given key
listDups' :: MDB_txn -> MDB_dbi' -> MDB_val -> IO [MDB_val]
listDups' txn dbi vKey = 
    alloca $ \ pKey ->
    alloca $ \ pVal ->
    withCursor' txn dbi $ \ crs -> do
    let loop l b = 
            if not b then return l else
            peek pVal >>= \ v ->
            mdb_cursor_get' MDB_NEXT_DUP crs pKey pVal >>= \ b' ->
            loop (v:l) b'
    poke pKey vKey
    b0 <- mdb_cursor_get' MDB_SET_KEY crs pKey pVal
    loop [] b0

withCursor' :: MDB_txn -> MDB_dbi' -> (MDB_cursor' -> IO a) -> IO a
withCursor' txn dbi = bracket g d where
    g = mdb_cursor_open' txn dbi
    d = mdb_cursor_close'
{-# INLINABLE withCursor' #-}

-- seek an exact match in the database. This will return Nothing in
-- one of these conditions:
--   (a) the candidate is not an exact match for the data
--   (b) the candidate has recently been GC'd from memory
-- Otherwise will return the necessary VRef.
candidateVRefInDB :: (VCacheable a) => VSpace -> MDB_txn -> MDB_val -> MDB_val -> IO (Maybe (VRef a))
candidateVRefInDB vc txn vData vCandidateAddr = do
    mbR <- mdb_get' txn (vcache_db_memory vc) vCandidateAddr
    case mbR of
        Nothing -> -- inconsistent hashmap and memory; this should never happen
            peekAddr vCandidateAddr >>= \ addr ->
            fail $ "VCache bug: undefined address " ++ show addr ++ " in hashmap" 
        Just vData' ->
            let bSameSize = mv_size vData == mv_size vData' in
            if not bSameSize then return Nothing else
            c_memcmp (mv_data vData) (mv_data vData') (mv_size vData) >>= \ o ->
            if (0 /= o) then return Nothing else
            peekAddr vCandidateAddr >>= \ addr -> -- exact match! But maybe GC'd.
            modifyMVarMasked (vcache_memory vc) $ \ m ->
                if (recentGC (mem_gc m) addr) then return (m, Nothing) else
                addr2vref' vc addr m >>= \ (m', vref) ->
                return (m', Just vref)



peekAddr :: MDB_val -> IO Address
peekAddr v =
    let expectedSize = fromIntegral (sizeOf (undefined :: Address)) in
    let bBadSize = expectedSize /= mv_size v in
    if bBadSize then fail "VCache bug: badly formed address" else
    peekAligned (castPtr (mv_data v))
{-# INLINABLE peekAddr #-}

   
seek :: (Monad m) => (a -> m (Maybe b)) -> [a] -> m (Maybe b)
seek _ [] = return Nothing
seek f (x:xs) = f x >>= continue where
    continue Nothing = seek f xs
    continue r@(Just _) = return r 

-- Intermediate data to allocate a new PVar. We don't know the
-- address yet, but we do know every other relevant aspect of
-- this PVar.
data AllocPVar a = AllocPVar
    { alloc_pvar_name :: !ByteString
    , alloc_pvar_data :: !ByteString
    , alloc_pvar_tvar :: !(TVar (RDV a))
    }

-- | Create a new, anonymous PVar as part of an active transaction.
-- Contents of the new PVar are not serialized unless the transaction
-- commits, though a placeholder may be allocated if the PVar remains
-- in memory during a write frame.
newPVar :: (VCacheable a) => a -> VTx (PVar a)
newPVar x = do
    vc <- getVTxSpace
    pvar <- liftSTM $ unsafeIOToSTM $ 
                newTVarIO (RDV x) >>= \ tvar ->
                modifyMVarMasked (vcache_memory vc) $
                allocPVar vc $ allocPlaceHolder BS.empty tvar
    markForWrite pvar x
    return pvar

allocPlaceHolder :: ByteString -> TVar (RDV a) -> AllocPVar a
allocPlaceHolder _name tvar = AllocPVar
    { alloc_pvar_name = _name
    , alloc_pvar_data = BS.singleton 0
    , alloc_pvar_tvar = tvar 
    }

-- | Create a new, anonymous PVar via the IO monad. This is similar
-- to `newTVarIO`, but not as well motivated: global PVars should
-- almost certainly be constructed as named, persistent roots. OTOH,
-- it may be useful for modeling persistent thunks or similar.
newPVarIO :: (VCacheable a) => VSpace -> a -> IO (PVar a)
newPVarIO vc x = do
    apv <- preAllocPVarIO vc BS.empty x
    pvar <- modifyMVarMasked (vcache_memory vc) $ allocPVar vc apv
    signalAlloc vc
    return pvar

preAllocPVarIO :: (VCacheable a) => VSpace -> ByteString -> a -> IO (AllocPVar a)
preAllocPVarIO vc _name x = do
    tvar <- newTVarIO (RDV x)
    ((), _data) <- runVPutIO vc (put x)
    return $! AllocPVar
        { alloc_pvar_name = _name
        , alloc_pvar_data = _data
        , alloc_pvar_tvar = tvar
        }

-- | Create an array of PVars with a given set of initial values. This
-- is equivalent to `mapM newPVar`, but guarantees adjacent addresses
-- in the persistence layer. This is mostly useful when working with 
-- large arrays, to simplify reasoning about paging performance.
newPVars :: (VCacheable a) => [a] -> VTx [PVar a]
newPVars [] = return []
newPVars xs = do
    vc <- getVTxSpace 
    pvars <- liftSTM $ unsafeIOToSTM $ do
        tvars <- mapM (newTVarIO . RDV) xs
        let apvs = fmap (allocPlaceHolder BS.empty) tvars 
        allocPVars vc apvs
    sequence_ (L.zipWith markForWrite pvars xs)
    return pvars

-- | Create an array of adjacent PVars via the IO monad. 
newPVarsIO :: (VCacheable a) => VSpace -> [a] -> IO [PVar a]
newPVarsIO _ [] = return []
newPVarsIO vc xs = do
    apvs <- mapM (preAllocPVarIO vc BS.empty) xs
    pvars <- allocPVars vc apvs
    signalAlloc vc
    return pvars

-- add a list of PVars to memory, acquiring addresses as we go.
allocPVars :: (VCacheable a) => VSpace -> [AllocPVar a] -> IO [PVar a]
allocPVars vc xs = 
    let step (m, vars) apv =
            allocPVar vc apv m >>= \ (m', pvar) ->
            return (m', pvar:vars)
    in
    modifyMVar (vcache_memory vc) $ \ m -> do
    (!m', lReversedPVars) <- foldM step (m,[]) xs
    return (m', L.reverse lReversedPVars)

allocPVar :: (VCacheable a) => VSpace -> AllocPVar a -> Memory -> IO (Memory, PVar a)
allocPVar = _allocPVar undefined
{-# INLINE allocPVar #-}

_allocPVar :: (VCacheable a) => a -> VSpace -> AllocPVar a -> Memory -> IO (Memory, PVar a)
_allocPVar _dummy !vc !apv !m = do
    let ty = typeOf _dummy
    let tvar = alloc_pvar_tvar apv
    let ac = mem_alloc m
    let addr = 1 + (alloc_new_addr ac)
    pveph <- mkPVEph vc addr tvar ty
    let pvar = PVar addr tvar vc ty put
    let _name = alloc_pvar_name apv 
    let _data = alloc_pvar_data apv
    let frm' = addPVarAlloc addr _name _data (alloc_frm_next ac)
    let pvars' = addPVEph pveph (mem_pvars m)
    let addr' = 2 + (alloc_new_addr ac)
    let ac' = ac { alloc_new_addr = addr', alloc_frm_next = frm' }
    let m' = m { mem_pvars = pvars', mem_alloc = ac' }
    return (m', pvar)

-- add new PVar to next allocation frame
addPVarAlloc :: Address -> ByteString -> ByteString -> AllocFrame -> AllocFrame
addPVarAlloc addr _name _data (AllocFrame l s r i) =
    let ul = Map.insert addr _data in
    let ur = if BS.null _name then id else (:) (addr,_name) in
    AllocFrame (ul l) s (ur r) i

-- | Global, persistent variables may be loaded by name. The name here
-- is prefixed by vcacheSubdir to control namespace collisions between
-- software components. These named variables are roots for GC purposes,
-- and will not be deleted.
--
-- Conceptually, a root PVar has always been there. Loading a root
-- is thus a pure computation. At the very least, it's an idempotent
-- operation. If the PVar exists, its value is lazily read from the
-- persistence layer. Otherwise, the given initial value is stored.
-- To reset a root PVar, simply write before reading.
-- 
-- The recommended practice for roots is to use only a few of them for
-- each persistent software component (i.e. each plugin, WAI app, etc.)
-- similarly to how a module might use just a few global variables. If
-- you need a dynamic set of variables, such as one per client, model
-- that explicitly using anonymous PVars. 
--
loadRootPVar :: (VCacheable a) => VCache -> ByteString -> a -> PVar a
loadRootPVar vc name ini = unsafePerformIO (loadRootPVarIO vc name ini)
{-# INLINE loadRootPVar #-}

-- | Load a root PVar in the IO monad. This is convenient to control 
-- where errors are detected or when initialization is performed.
-- See loadRootPVar.
loadRootPVarIO :: (VCacheable a) => VCache -> ByteString -> a -> IO (PVar a)
loadRootPVarIO vc !name ini =
    case vcacheSubdirM name vc of
        Just (VCache vs path) -> _loadRootPVarIO vs path ini
        Nothing ->
            let path = vcache_path vc `BS.append` name in
            fail $  "VCache: root PVar path too long: " ++ show path

-- find the PVar by name in the database or recent allocations. If
-- not in those locations, go ahead and construct it new.
_loadRootPVarIO :: (VCacheable a) => VSpace -> ByteString -> a -> IO (PVar a)
_loadRootPVarIO vc !_name ini = withRdOnlyTxn vc $ \ txn ->
    withByteStringVal _name $ \ rootKey ->
    mdb_get' txn (vcache_db_vroots vc) rootKey >>= \ mbRoot ->
    case mbRoot of
        Just val -> peekAddr val >>= addr2pvar vc -- found root in database
        Nothing -> -- allocate new PVar or find in recent allocations
            preAllocPVarIO vc _name ini >>= \ apv -> -- common case is new var
            modifyMVarMasked (vcache_memory vc) $ \ m ->
                let ff = L.find ((== _name) . snd) . alloc_root in
                case allocFrameSearch ff (mem_alloc m) of
                    Just rn -> addr2pvar' vc (fst rn) m -- recent allocation
                    Nothing -> signalAlloc vc >> allocPVar vc apv m -- new root PVar
{-# NOINLINE _loadRootPVarIO #-}
 
-- report allocation to the writer thread
signalAlloc :: VSpace -> IO ()
signalAlloc vc = void $ tryPutMVar (vcache_signal vc) () 

foreign import ccall unsafe "string.h memcmp" c_memcmp
    :: Ptr Word8 -> Ptr Word8 -> CSize -> IO CInt

