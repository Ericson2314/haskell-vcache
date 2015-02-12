{-# LANGUAGE ForeignFunctionInterface, BangPatterns #-}

-- | Constructors and Allocators for VRefs and PVars.
--
-- This module is the nexus of a lot of concurrency concerns. VRefs
-- are GC'd, but might concurrently be revived by structure sharing,
-- so we arbitrate GC vs. revival.
--
-- New allocations might not be seen at the LMDB-layer by the older
-- readers, so we must track recent allocations.
--
-- Because VRefs are frequently constructed by unsafePerformIO, none
-- of these operations can use STM (excepting newTVarIO).
--
-- For now, I'm just using a global lock on these operations, via MVar.
-- Since writes are also single-threaded, and serialization and such 
-- can operate outside the lock, this shouldn't be a bottleneck. But 
-- it might cause a little more context switching than desirable.
-- 
module Database.VCache.Alloc
    ( addr2vref
    , addr2pvar
    , newVRefIO
    , newVRefIO'
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
addr2vref :: (VCacheable a) => VSpace -> Address -> IO (VRef a)
addr2vref vc addr = 
    assert (isVRefAddr addr) $ 
    modifyMVarMasked (vcache_memory vc) $ 
    addr2vref' vc addr
{-# INLINE addr2vref #-}

addr2vref' :: (VCacheable a) => VSpace -> Address -> Memory -> IO (Memory, VRef a)
addr2vref' vc addr m = _addr2vref undefined vc addr m
{-# INLINE addr2vref' #-}

_addr2vref :: (VCacheable a) => a -> VSpace -> Address -> Memory -> IO (Memory, VRef a)
_addr2vref _dummy vc addr m = do
    let ty = typeOf _dummy
    let em = mem_vrefs m
    let mbf = Map.lookup addr em >>= Map.lookup ty
    mbCache <- mbrun _getVREphCache mbf 
    case mbCache of 
        Just cache -> return (m, VRef addr cache vc get)
        Nothing -> do
            cache <- newIORef NotCached
            wkCache <- mkWeakIORef cache (return ())
            let e  = VREph addr ty wkCache
            let m' = m { mem_vrefs = addVREph e em }
            m' `seq` return (m', VRef addr cache vc get)
{-# NOINLINE _addr2vref #-}

addVREph :: VREph -> VREphMap -> VREphMap
addVREph e = Map.alter (Just . maybe i0 ins) (vreph_addr e) where
    ty = vreph_type e
    i0 = Map.singleton ty e
    ins = Map.insert ty e
{-# INLINABLE addVREph #-}

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

-- | Obtain a PVar given an address. PVar will lazily load when read.
-- This operation does not try to read the database. It may fail if
-- the requested address has already been loaded with another type.
addr2pvar :: (VCacheable a) => VSpace -> Address -> IO (PVar a)
addr2pvar vc addr = 
    assert (isPVarAddr addr) $ 
    modifyMVarMasked (vcache_memory vc) $ 
    addr2pvar' vc addr
{-# INLINE addr2pvar #-}

addr2pvar' :: (VCacheable a) => VSpace -> Address -> Memory -> IO (Memory, PVar a) 
addr2pvar' vc addr m = _addr2pvar undefined vc addr m
{-# INLINE addr2pvar' #-}

_addr2pvar :: (VCacheable a) => a -> VSpace -> Address -> Memory -> IO (Memory, PVar a)
_addr2pvar _dummy vc addr m = do
    let ty = typeOf _dummy
    let mpv = mem_pvars m
    let mbf = Map.lookup addr mpv
    mbVar <- (`mbrun` mbf) $ \ pve -> do
        let tye = pveph_type pve
        unless (ty == tye) (fail $ eTypeMismatch addr ty tye)
        _getPVEphTVar pve
    case mbVar of
        Just var -> return (m, PVar addr var vc put)
        Nothing -> do
            lzv <- unsafeInterleaveIO $ RDV . fst <$> readAddrIO vc addr get
            var <- newTVarIO lzv
            wkVar <- mkWeakTVar var (return ())
            let e = PVEph addr ty wkVar
            let m' = m { mem_pvars = addPVEph e mpv }
            m' `seq` return (m', PVar addr var vc put)
{-# NOINLINE _addr2pvar #-}

eTypeMismatch :: Address -> TypeRep -> TypeRep -> String
eTypeMismatch addr tyNew tyOld = 
    showString "PVar user error: address " . shows addr .
    showString " type mismatch on load. " .
    showString " Existing: " . shows tyOld .
    showString " Expecting: " . shows tyNew $ ""

addPVEph :: PVEph -> PVEphMap -> PVEphMap
addPVEph pve = Map.insert (pveph_addr pve) pve
{-# INLINE addPVEph #-}

-- unsafe: get data, assuming that type already matches.
_getPVEphTVar :: PVEph -> IO (Maybe (TVar (RDV a)))
_getPVEphTVar = Weak.deRefWeak . _u where
    _u :: PVEph -> Weak (TVar (RDV a))
    _u (PVEph { pveph_data = w }) = _c w
    _c :: Weak (TVar (RDV b)) -> Weak (TVar (RDV a))
    _c = unsafeCoerce
{-# INLINE _getPVEphTVar #-}

-- | Construct a new VRef and initialize cache with given value.
newVRefIO :: (VCacheable a) => VSpace -> a -> CacheMode -> IO (VRef a)
newVRefIO vc v cm = 
    runVPutIO vc (put v) >>= \ ((), _data, _deps) ->
    allocVRefIO vc _data _deps >>= \ vref ->
    let w = cacheWeight (BS.length _data) (L.length _deps) in
    atomicModifyIORef (vref_cache vref) (initVRefCache v w cm) >>= \ () ->
    return vref
{-# NOINLINE newVRefIO #-}

-- | initialize a VRef cache with a known value (no need to read the
-- database). If the cache already exists, the existing value is not
-- modified but the cache is touched with the requested cache mode as
-- if dereferenced.
initVRefCache :: a -> Int -> CacheMode -> Cache a -> (Cache a, ())
initVRefCache v w cm c = (c', c' `seq` ()) where
    c' = case c of 
        NotCached -> mkVRefCache v w cm
        Cached r b -> Cached r (touchCache cm b) 
{-# INLINABLE initVRefCache #-}

-- | Construct a new VRef without initializing the cache.
newVRefIO' :: (VCacheable a) => VSpace -> a -> IO (VRef a) 
newVRefIO' vc v =
    runVPutIO vc (put v) >>= \ ((), _data, _deps) ->
    allocVRefIO vc _data _deps 
{-# INLINE newVRefIO' #-}

-- | Allocate a VRef given data and dependencies.
--
-- We'll try to find an existing match in the database, then from the
-- recent allocations list, ignoring addresses that have recently been
-- GC'd (to account for reading old data).
--
-- If a match is discovered, we'll use the existing address. Otherwise,
-- we'll allocate a new one and leave it to the background writer thread.
--
allocVRefIO :: (VCacheable a) => VSpace -> ByteString -> [PutChild] -> IO (VRef a)
allocVRefIO vc _data _deps = 
    let _name = hash _data in
    withByteStringVal _name $ \ vName ->
    withByteStringVal _data $ \ vData ->
    withRdOnlyTxn vc $ \ txn ->
    listDups' txn (vcache_db_caddrs vc) vName >>= \ caddrs ->
    seek (candidateVRefInDB vc txn vData) caddrs >>= \ mbVRef ->
    case mbVRef of
        Just !vref -> return vref -- found matching VRef in database
        Nothing -> modifyMVarMasked (vcache_memory vc) $ \ m ->
            let okAddr addr = isVRefAddr addr && not (recentGC (mem_gc m) addr) in
            let match an = okAddr (alloc_addr an) && (_data == (alloc_data an)) in
            let ff frm = Map.lookup _name (alloc_seek frm) >>= L.find match in
            case allocFrameSearch ff (mem_alloc m) of
                Just an -> addr2vref' vc (alloc_addr an) m -- found among recent allocations
                Nothing -> do -- allocate a new VRef address
                    let ac = mem_alloc m
                    let addr = alloc_new_addr ac
                    let an = Allocation { alloc_name = _name, alloc_data = _data
                                        , alloc_deps = _deps, alloc_addr = addr }
                    let frm' = addToFrame an (alloc_frm_next ac) 
                    let ac' = ac { alloc_new_addr = 2 + addr, alloc_frm_next = frm' }
                    let m' = m { mem_alloc = ac' }
                    signalAlloc vc
                    addr2vref' vc addr m'
{-# NOINLINE allocVRefIO #-}

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


-- add annotation to frame without seek
addToFrame :: Allocation -> AllocFrame -> AllocFrame
addToFrame an frm 
 | BS.null (alloc_name an) = -- anonymous PVars 
    assert (isPVarAddr (alloc_addr an)) $
    let list' = Map.insert (alloc_addr an) an (alloc_list frm) in
    frm { alloc_list = list' }
 | otherwise = -- root PVars or hashed VRefs 
    let list' = Map.insert (alloc_addr an) an (alloc_list frm) in
    let add_an = Just . (an:) . maybe [] id in
    let seek' = Map.alter add_an (alloc_name an) (alloc_seek frm) in
    frm { alloc_list = list', alloc_seek = seek' }

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

-- | Create a new, anonymous PVar as part of an active transaction.
-- Contents of the new PVar are not serialized unless the transaction
-- commits (though a placeholder is still allocated). 
newPVar :: (VCacheable a) => a -> VTx (PVar a)
newPVar x = newPVars [x] >>= takeSingleton "newPVar"

-- | Create a new, anonymous PVar via the IO monad. This is similar
-- to `newTVarIO`, but not as well motivated: global PVars should
-- almost certainly be constructed as named, persistent roots. 
-- 
newPVarIO :: (VCacheable a) => VSpace -> a -> IO (PVar a)
newPVarIO vc x = newPVarsIO vc [x] >>= takeSingleton "newPVarIO"

takeSingleton :: (Monad m) => String -> [a] -> m a
takeSingleton _ [a] = return a
takeSingleton eMsg _ = fail $ "expecting single value; " ++ eMsg 
{-# INLINE takeSingleton #-}

-- | Create an array of PVars with a given set of initial values. This
-- is equivalent to `mapM newPVar`, but guarantees adjacent addresses,
-- which will correspond to adjacency at the LMDB layer and may improve
-- memory locality and paging behavior.
newPVars :: (VCacheable a) => [a] -> VTx [PVar a]
newPVars [] = return []
newPVars xs = do
    vc <- getVTxSpace 
    pvars <- liftSTM $ unsafeIOToSTM $ _newPVars vc xs
    sequence_ (L.zipWith markForWrite pvars xs)
    return pvars

_newPVars :: (VCacheable a) => VSpace -> [a] -> IO [PVar a]
_newPVars vc xs = mapM prePVar xs >>= _allocPVars vc where 
    prePVar x = do
        tvar <- newTVarIO (RDV x)
        weak <- mkWeakTVar tvar (return ())
        return $! AllocPVar
            { alloc_pvar_name = BS.empty
            , alloc_pvar_data = BS.singleton 0 -- placeholder
            , alloc_pvar_deps = [] -- placeholder
            , alloc_pvar_tvar = tvar
            , alloc_pvar_weak = weak
            }

-- | Create an array of adjacent PVars via the IO monad. 
newPVarsIO :: (VCacheable a) => VSpace -> [a] -> IO [PVar a]
newPVarsIO _ [] = return []
newPVarsIO vc xs = withSignal $ mapM prePVar xs >>= _allocPVars vc where
    prePVar x = do
        tvar <- newTVarIO (RDV x)
        weak <- mkWeakTVar tvar (return ())
        ((), _data, _deps) <- runVPutIO vc (put x)
        return $! AllocPVar
            { alloc_pvar_name = BS.empty
            , alloc_pvar_data = _data
            , alloc_pvar_deps = _deps
            , alloc_pvar_tvar = tvar
            , alloc_pvar_weak = weak
            }
    withSignal op = do { r <- op; signalAlloc vc; return r } 

-- Intermediate data to allocate a PVar. We don't know the
-- address yet, but we do know everything else about this PVar.
data AllocPVar a = AllocPVar
    { alloc_pvar_name :: !ByteString
    , alloc_pvar_data :: !ByteString
    , alloc_pvar_deps :: ![PutChild]
    , alloc_pvar_tvar :: !(TVar (RDV a))
    , alloc_pvar_weak :: !(Weak (TVar (RDV a)))
    }

-- add a list of PVars to memory, acquiring addresses as we go.
_allocPVars :: (VCacheable a) => VSpace -> [AllocPVar a] -> IO [PVar a]
_allocPVars vc xs =
    modifyMVar (vcache_memory vc) $ \ m ->
    return $! L.mapAccumL (fnAllocPVar vc) m xs

-- allocate an address for just one new PVar.
fnAllocPVar :: (VCacheable a) => VSpace -> Memory -> AllocPVar a -> (Memory, PVar a)
fnAllocPVar vc m apv = _fnAllocPVar undefined vc m apv 
{-# INLINE fnAllocPVar #-}

_fnAllocPVar :: (VCacheable a) => a -> VSpace -> Memory -> AllocPVar a -> (Memory, PVar a)
_fnAllocPVar _dummy vc m apv = m' `seq` pvar `seq` (m', pvar) where
    ac = mem_alloc m
    pvAddr = 1 + (alloc_new_addr ac)
    an = Allocation 
        { alloc_name = alloc_pvar_name apv
        , alloc_data = alloc_pvar_data apv
        , alloc_addr = pvAddr
        , alloc_deps = alloc_pvar_deps apv
        }
    pvar = PVar pvAddr (alloc_pvar_tvar apv) vc put
    pveph = PVEph pvAddr (typeOf _dummy) (alloc_pvar_weak apv)
    addr' = 2 + alloc_new_addr ac
    frm' = addToFrame an (alloc_frm_next ac)
    ac' = ac { alloc_new_addr = addr', alloc_frm_next = frm' }
    mpv' = addPVEph pveph (mem_pvars m)
    m' = m { mem_alloc = ac', mem_pvars = mpv' }



-- | Global, persistent variables may be loaded by name. The name here
-- is prefixed by vcacheSubdir to control namespace collisions between
-- software components. These named variables are roots for GC purposes,
-- and will not be deleted.
--
-- Conceptually, the root PVar has always been there. Loading a root
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
loadRootPVarIO vc name ini =
    case vcacheSubdirM name vc of
        Just (VCache vs path) -> _loadRootPVarIO vs path ini
        Nothing ->
            let path = vcache_path vc `BS.append` name in
            fail $  "VCache: root PVar path too long: " ++ show path

_loadRootPVarIO :: (VCacheable a) => VSpace -> ByteString -> a -> IO (PVar a)
_loadRootPVarIO vc _name ini = withRdOnlyTxn vc $ \ txn ->
    withByteStringVal _name $ \ rootKey ->
    mdb_get' txn (vcache_db_vroots vc) rootKey >>= \ mbRoot ->
    case mbRoot of
        Just val -> peekAddr val >>= addr2pvar vc -- found root in database
        Nothing -> -- allocate root
            runVPutIO vc (put ini) >>= \ ((), _data, _deps) ->
            join $ modifyMVarMasked (vcache_memory vc) $ \ m ->
                let match an = isPVarAddr (alloc_addr an) in
                let ff frm = Map.lookup _name (alloc_seek frm) >>= L.find match in
                case allocFrameSearch ff (mem_alloc m) of
                    Just an -> return (m, addr2pvar vc (alloc_addr an)) -- found in recent allocations 
                    Nothing -> do
                        tvar <- newTVarIO (RDV ini)
                        weak <- mkWeakTVar tvar (return ())
                        let apv = AllocPVar
                                { alloc_pvar_name = _name
                                , alloc_pvar_data = _data
                                , alloc_pvar_deps = _deps
                                , alloc_pvar_tvar = tvar
                                , alloc_pvar_weak = weak
                                }
                        let (m', pvar) = fnAllocPVar vc m apv
                        return (m', signalAlloc vc >> return pvar)
{-# NOINLINE _loadRootPVarIO #-}
 
-- report allocation to the writer thread
signalAlloc :: VSpace -> IO ()
signalAlloc vc = void $ tryPutMVar (vcache_signal vc) () 

foreign import ccall unsafe "string.h memcmp" c_memcmp
    :: Ptr Word8 -> Ptr Word8 -> CSize -> IO CInt

