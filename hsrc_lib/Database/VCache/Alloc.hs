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
import Database.VCache.RWLock

-- | Obtain a VRef given an address and value. Not initially cached.
--
-- This operation will return Nothing if the requested address has
-- been selected for garbage collection. Note that this operation 
-- does not touch the LMDB-layer database.
addr2vref :: (VCacheable a) => VSpace -> Address -> IO (VRef a)
addr2vref vc addr = assert (isVRefAddr addr) $ _addr2vref undefined vc addr 
{-# INLINABLE addr2vref #-}

mbrun :: (Applicative m) => (a -> m (Maybe b)) -> Maybe a -> m (Maybe b)
mbrun _ Nothing = pure Nothing
mbrun op (Just a) = op a
{-# INLINE mbrun #-}

_addr2vref :: (VCacheable a) => a -> VSpace -> Address -> IO (VRef a)
_addr2vref _dummy vc addr = modifyMVarMasked (vcache_memory vc) $ \ m -> do
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

-- | Obtain a PVar given an address. PVar will lazily load when read.
-- This operation does not try to read the database.
addr2pvar :: (VCacheable a) => VSpace -> Address -> IO (PVar a)
addr2pvar vc addr = assert (isPVarAddr addr) $ _addr2pvar undefined vc addr
{-# INLINE addr2pvar #-}

_addr2pvar :: (VCacheable a) => a -> VSpace -> Address -> IO (PVar a)
_addr2pvar _dummy vc addr = modifyMVarMasked (vcache_memory vc) $ \ m -> do
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
            return (m', PVar addr var vc put)
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
-- modified and the cache is touched with the requested cache mode.
initVRefCache :: a -> Int -> CacheMode -> Cached a -> (Cached a, ())
initVRefCache v w cm c = (c', c' `seq` ()) where
    c' = case c of 
        NotCached -> mkVRefCache v w cm
        Cached r b -> Cached r (touchCache cm b) 
{-# INLINE initVRefCache #-}

-- | Construct a new VRef without initializing the cache.
newVRefIO' :: (VCacheable a) => VSpace a -> IO (VRef a) 
newVRefIO' vc v =
    runVPutIO vc (put v) >>= \ ((), _data, _deps) ->
    allocVRefIO vc _data _deps 
{-# INLINE newVRefIO' #-}

-- | Allocate a VRef given data and dependencies.
--
-- We'll try to find an existing match in the database, modulo those 
-- in the GC lists. If such a match is discovered, we'll reuse the 
-- existing address. Otherwise, we allocate a new one.
--
allocVRefIO :: (VCacheable a) => VSpace -> ByteString -> [PutChild] -> IO (VRef a)
allocVRefIO vc _data _deps = 
    let _name = hash _data in
    withByteStringVal _name $ \ vName ->
    withByteStringVal _data $ \ vData ->
    withRdOnlyTxn vc $ \ txn ->
    listDups' txn (vcache_db_caddrs vc) vName >>= \ caddrs ->
    seek (matchCandidate vc txn vData) caddrs >>= \ mbFound ->
    case mbFound of
        Just !vref -> return vref
        Nothing -> 
            let acRef = vcache_allocator vc in
            let allocFn = allocVRef _name _data _deps in
            atomicModifyIORef acRef allocFn >>= \ !addr ->
            addr2vref vc addr >>= \ mbv -> case mbv of
                Nothing -> fail $ "VRef bug: must not GC recent allocations!"
                Just !vref -> signalAlloc vc >> return vref 
{-# NOINLINE allocVRefIO #-}



-- | Construct a new PVar within a VTx transaction. Nothing will be
-- written to disk regarding this PVar until the transaction completes. 
-- 
newPVar = error "TODO: newPVar"

-- | Construct multiple PVars within a VTx transaction. Nothing will
-- be written to disk 
newPVars = error "TODO: newPVars"
newPVarIO = error "TODO: newPVarIO"
loadRootPVar = error "TODO: loadRootPVar"
loadRootPVarIO = error "TODO: loadRootPVarIO"

{-




-- | load a root PVar immediately, in the IO monad. This can be more
-- convenient than loadRootPVar if you need to control where errors
-- are detected or when initialization is performed. See loadRootPVar.
loadRootPVarIO :: (VCacheable a) => VCache -> ByteString -> a -> IO (PVar a)
loadRootPVarIO vc name def =
    case vcacheSubdirM name vc of
        Just (VCache vs path) -> _loadRootPVarIO vs path def
        Nothing -> 
            let fullName = vcache_path vc `BS.append` name in
            let eMsg = "root PVar name too long: " ++ show fullName in
            fail ("VCache: " ++ eMsg)

_loadRootPVarIO :: (VCacheable a) => VSpace -> ByteString -> a -> IO (PVar a)
_loadRootPVarIO vc name defaultVal = withRdOnlyTxn vc $ \ txn ->
    withByteStringVal name $ \ rootKey ->
    mdb_get' txn (vcache_db_vroots vc) rootKey >>= \ mbRoot ->
    case mbRoot of
        Just val -> -- this root has been around for a while
            let szAddr = sizeOf (undefined :: Address) in
            let bBadSize = (szAddr /= fromIntegral (mv_size val)) in
            let eMsg = "corrupt data for PVar root " ++ show name in
            if bBadSize then fail eMsg else
            peekAligned (castPtr (mv_data val)) >>= \ addr ->
            addr2pvar vc addr
        Nothing -> -- first use OR allocated very recently
            runVPutIO vc (put defaultVal) >>= \ ((),_data,_deps) ->
            let acRef = vcache_allocator vc in
            let allocFn = allocNamedPVar name _data _deps in
            atomicModifyIORef acRef allocFn >>= \ addr ->
            signalAlloc vc >>
            addr2pvar vc addr

-- This variation of PVar allocation will support lookup by the given
-- PVar name, i.e. in case the same root is allocated multiple times in
-- a short period.
allocNamedPVar :: ByteString -> ByteString -> [PutChild] -> Allocator -> (Allocator, Address)
allocNamedPVar _name _data _deps ac =
    let match an = isPVarAddr (alloc_addr an) in
    let ff frm = Map.lookup _name (alloc_seek frm) >>= L.find match in
    case allocFrameSearch ff ac of
        Just an -> (ac, alloc_addr an) -- allocated very recently
        Nothing ->
            let newAddr = alloc_new_addr ac in
            let an = Allocation { alloc_name = _name
                                , alloc_data = _data
                                , alloc_deps = _deps
                                , alloc_addr = 1 + newAddr
                                }
            in
            let frm' = addToFrame an (alloc_frm_next ac) in
            let ac' = ac { alloc_new_addr = 2 + newAddr, alloc_frm_next = frm' } in
            (ac', alloc_addr an)

-- | Load a root PVar associated with a VCache. The given name will be
-- implicitly prefixed based on vcacheSubdir - a simple append operation.
-- If this is the first time the PVar is loaded, it will be initialized 
-- by the given value. 
--
-- The illusion provided is that the PVar has always been there, accessible
-- by name. Thus, this is provided as a pure operation. However, errors are
-- possible - e.g. you cannot load a PVar with two different types, and the
-- limit for the PVar name is about 500 bytes. loadRootPVarIO can provide
-- better control over when and where errors are detected. 
--
-- The normal use case for root PVars is to have just a few near the toplevel
-- of the application, or in each major modular component (e.g. a few for the
-- framework, a few for the WAI app, a few for the plugin). Use vcacheSubdir 
-- to divide the namespace among modular components. Domain modeling should 
-- be pushed to the data types.
--
loadRootPVar :: (VCacheable a) => VCache -> ByteString -> a -> PVar a
loadRootPVar vc name ini = unsafePerformIO (loadRootPVarIO vc name ini)
{-# INLINE loadRootPVar #-}

-- | Create a new, unique, anonymous PVar via the IO monad. This is
-- more or less equivalent to newTVarIO, except that the PVar may be
-- referenced from persistent values in the associated VCache. Like
-- newTVarIO, this can be used from unsafePerformIO. Unlike newTVarIO, 
-- there is no use case for newPVarIO to construct global variables
-- (because loadRootPVar fulfills that role).
--
-- In most cases, you should favor the transactional newPVar.
--
newPVarIO :: (VCacheable a) => VSpace -> a -> IO (PVar a)
newPVarIO vc ini = 
    runVPutIO vc (put ini) >>= \ ((), _data, _deps) ->
    atomicModifyIORef (vcache_allocator vc) (allocAnonPVarIO _data _deps) >>= \ addr ->
    signalAlloc vc >>
    addr2pvar_new vc addr ini

-- allocate PVar such that its initial value will be stored by the background
-- thread. This variation assumes an anonymous PVar.
allocAnonPVarIO :: ByteString -> [PutChild] -> Allocator -> (Allocator, Address)
allocAnonPVarIO _data _deps ac =
    let newAddr = alloc_new_addr ac in
    let an = Allocation { alloc_name = BS.empty
                        , alloc_data = _data
                        , alloc_deps = _deps
                        , alloc_addr = 1 + newAddr
                        }
    in
    let frm' = addToFrame an (alloc_frm_next ac) in
    let ac' = ac { alloc_new_addr = 2 + newAddr, alloc_frm_next = frm' } in
    (ac', alloc_addr an)
 
-- | Create a new, anonymous PVar as part of an active transaction.
--
-- Compared to newPVarIO, this transactional variation has performance
-- advantages. The constructor only needs to grab an address. Nothing is
-- written to disk unless the transaction succeeds. 
newPVar :: (VCacheable a) => a -> VTx (PVar a)
newPVar ini = do
    vc <- getVTxSpace 
    pvar <- liftSTM $ unsafeIOToSTM $ newPVarVTxIO vc ini
    markForWrite pvar ini
    return pvar

newPVarVTxIO :: (VCacheable a) => VSpace -> a -> IO (PVar a)
newPVarVTxIO vc ini = 
    atomicModifyIORef (vcache_allocator vc) allocPVarVTx >>= \ addr ->
    -- no need to signal allocator in this particular case
    addr2pvar_new vc addr ini
{-# INLINE newPVarVTxIO #-}

-- Allocate just the new PVar address. Do not store any information
-- about this PVar in the allocator. Writing the PVar will be delayed
-- into the normal write process, i.e. as part of the VTx context.
allocPVarVTx :: Allocator -> (Allocator, Address)
allocPVarVTx ac = 
    let newAddr = alloc_new_addr ac in
    let pvAddr = 1 + newAddr in
    let ac' = ac { alloc_new_addr = 2 + newAddr } in
    (ac', pvAddr)
   

   
seek :: (Monad m) => (a -> m (Maybe b)) -> [a] -> m (Maybe b)
seek _ [] = return Nothing
seek f (x:xs) = f x >>= continue where
    continue Nothing = seek f xs
    continue r@(Just _) = return r 

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

-- seek an exact match in the database. This will return Nothing in
-- one of these conditions:
--   (a) the candidate is not an exact match for the data
--   (b) the candidate has recently been GC'd from memory
-- Otherwise will return the necessary VRef.
matchCandidate :: (VCacheable a) => VSpace -> MDB_txn -> MDB_val -> MDB_val -> IO (Maybe (VRef a))
matchCandidate vc txn vData vCandidateAddr = do
    mbR <- mdb_get' txn (vcache_db_memory vc) vCandidateAddr
    case mbR of
        Nothing -> -- inconsistent hashmap and memory, should never happen
            peekAddr vCandidateAddr >>= \ addr ->
            fail $ "VCache bug: undefined address " ++ show addr ++ " in hashmap" 
        Just vData' ->
            let bSameSize = mv_size vData == mv_size vData' in
            if not bSameSize then return Nothing else
            c_memcmp (mv_data vData) (mv_data vData') (mv_size vData) >>= \ o ->
            if (0 /= o) then return Nothing else
            peekAddr vCandidateAddr >>= \ addr ->
            addr2vref vc addr

withCursor' :: MDB_txn -> MDB_dbi' -> (MDB_cursor' -> IO a) -> IO a
withCursor' txn dbi = bracket g d where
    g = mdb_cursor_open' txn dbi
    d = mdb_cursor_close'
{-# INLINABLE withCursor' #-}

peekAddr :: MDB_val -> IO Address
peekAddr v =
    let expectedSize = fromIntegral (sizeOf (undefined :: Address)) in
    let bBadSize = expectedSize /= mv_size v in
    if bBadSize then fail "VCache bug: badly formed address in hashmap" else
    peekAligned (castPtr (mv_data v))
{-# INLINABLE peekAddr #-}

-- allocate a VRef. This will also search the recently allocated addresses for
-- a potential match in case an identical VRef was very recently allocated!
allocVRef :: ByteString -> ByteString -> [PutChild] -> Allocator -> (Allocator, Address)
allocVRef _name _data _deps ac =
    let match an = isVRefAddr (alloc_addr an) && (_data == (alloc_data an)) in
    let ff frm = Map.lookup _name (alloc_seek frm) >>= L.find match in
    case allocFrameSearch ff ac of
        Just an -> (ac, alloc_addr an)
        Nothing ->
            let newAddr = alloc_new_addr ac in
            let an = Allocation { alloc_name = _name
                                , alloc_data = _data
                                , alloc_deps = _deps
                                , alloc_addr = newAddr
                                }
            in
            let frm' = addToFrame an (alloc_frm_next ac) in
            let ac' = ac { alloc_new_addr = 2 + newAddr, alloc_frm_next = frm' } in
            (ac', newAddr)

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

-- report allocation to the writer threads
signalAlloc :: VSpace -> IO ()
signalAlloc vc = void $ tryPutMVar (vcache_signal vc) () 

foreign import ccall unsafe "string.h memcmp" c_memcmp
    :: Ptr Word8 -> Ptr Word8 -> CSize -> IO CInt


-}