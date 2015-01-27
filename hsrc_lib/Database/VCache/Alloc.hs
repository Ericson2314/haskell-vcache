{-# LANGUAGE ForeignFunctionInterface #-}

-- allocators for PVars and VRefs
module Database.VCache.Alloc
    ( loadRootPVar
    , loadRootPVarIO
    , newPVar
    , newPVarIO
    , newVRefIO
    , newVRefIO'
    ) where

import Control.Exception
import Control.Monad
import Data.Word
import Data.IORef
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.List as L
import qualified Data.Map.Strict as Map
import qualified Data.IntMap.Strict as IntMap
import Foreign.C.Types
import Foreign.Ptr
import Foreign.Storable
import Foreign.Marshal.Alloc
import GHC.Conc (unsafeIOToSTM)
import System.IO.Unsafe (unsafePerformIO)

import Database.LMDB.Raw

import Database.VCache.Types
import Database.VCache.FromAddr
import Database.VCache.Path
import Database.VCache.Aligned
import Database.VCache.VPutFini
import Database.VCache.Hash


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
            atomicModifyIORef (vcache_allocator vc) (allocNamedPVar name _data _deps) >>= \ addr ->
            addr2pvar vc addr

-- This variation of PVar allocation will support lookup by the given
-- PVar name, i.e. in case the same root is allocated multiple times in
-- a short period.
allocNamedPVar :: ByteString -> ByteString -> [PutChild] -> Allocator -> (Allocator, Address)
allocNamedPVar _name _data _deps ac =
    let hkey = allocNameHash 1 _name in
    let match an = isPVarAddr (alloc_addr an) && (_name == (alloc_name an)) in
    let ff frm = IntMap.lookup hkey (alloc_seek frm) >>= L.find match in
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
            let frm' = addToFrameS an hkey (alloc_frm_next ac) in
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
-- there is no use case for newPVarIO to construct global variables. 
--
-- In most cases, you should favor the transactional newPVar.
--
newPVarIO :: (VCacheable a) => VSpace -> a -> IO (PVar a)
newPVarIO vc ini = 
    runVPutIO vc (put ini) >>= \ ((), _data, _deps) ->
    atomicModifyIORef (vcache_allocator vc) (allocAnonPVarIO _data _deps) >>= \ addr ->
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
    markForWrite pvar
    return pvar

newPVarVTxIO :: (VCacheable a) => VSpace -> a -> IO (PVar a)
newPVarVTxIO vc ini = 
    atomicModifyIORef (vcache_allocator vc) allocPVarVTx >>= \ addr ->
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
   
-- | Construct a VRef and cache the value (if new)
newVRefIO :: (VCacheable a) => VSpace -> a -> CacheMode -> IO (VRef a)
newVRefIO vc v cm =
    runVPutIO vc (put v) >>= \ ((), _data, _deps) ->
    allocVRefIO vc _data _deps >>= \ addr ->
    let w = cacheWeight (BS.length _data) (L.length _deps) in
    let c0 = mkVRefCache v w cm in
    addr2vref vc addr c0

-- | Construct a VRef with initially empty cache (if new)
newVRefIO' :: (VCacheable a) => VSpace -> a -> IO (VRef a)
newVRefIO' vc v = 
    runVPutIO vc (put v) >>= \ ((), _data, _deps) ->
    allocVRefIO vc _data _deps >>= \ addr ->
    addr2vref vc addr NotCached

allocVRefIO :: VSpace -> ByteString -> [PutChild] -> IO Address
allocVRefIO vc _data _deps = 
    let _name = BS.take 8 $ hash _data in
    withByteStringVal _name $ \ vName ->
    withByteStringVal _data $ \ vData ->
    withRdOnlyTxn vc $ \ txn ->
    listDups' txn (vcache_db_caddrs vc) vName >>= \ caddrs ->
    seek (matchCandidate vc txn vData) caddrs >>= \ mbFound ->
    case mbFound of
        Nothing -> atomicModifyIORef (vcache_allocator vc) (allocVRef _name _data _deps)
        Just addr -> return addr
   
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

-- seek an exact match in the database
matchCandidate :: VSpace -> MDB_txn -> MDB_val -> MDB_val -> IO (Maybe Address)
matchCandidate vc txn vData vCandidateAddr = do
    mbR <- mdb_get' txn (vcache_db_memory vc) vCandidateAddr
    case mbR of
        Nothing -> -- this should not happen
            fail "corrupt database: undefined address in hashmap"
        Just vData' ->
            let bSameSize = mv_size vData == mv_size vData' in
            if not bSameSize then return Nothing else
            c_memcmp (mv_data vData) (mv_data vData') (mv_size vData) >>= \ o ->
            if (0 /= o) then return Nothing else
            liftM Just (peekAddr vCandidateAddr)

withCursor' :: MDB_txn -> MDB_dbi' -> (MDB_cursor' -> IO a) -> IO a
withCursor' txn dbi = bracket g d where
    g = mdb_cursor_open' txn dbi
    d = mdb_cursor_close'

peekAddr :: MDB_val -> IO Address
peekAddr v =
    let expectedSize = fromIntegral (sizeOf (undefined :: Address)) in
    let bBadSize = expectedSize /= mv_size v in
    if bBadSize then fail "corrupt database: badly formed address in hashmap" else
    peekAligned (castPtr (mv_data v))

-- allocate a VRef. This will also search the recently allocated addresses for
-- a potential match in case an identical VRef was very recently allocated!
allocVRef :: ByteString -> ByteString -> [PutChild] -> Allocator -> (Allocator, Address)
allocVRef _name _data _deps ac =
    let hkey = allocNameHash 1 _name in
    let match an = isVRefAddr (alloc_addr an) && (_data == (alloc_data an)) in
    let ff frm = IntMap.lookup hkey (alloc_seek frm) >>= L.find match in
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
            let frm' = addToFrameS an hkey (alloc_frm_next ac) in
            let ac' = ac { alloc_new_addr = 2 + newAddr, alloc_frm_next = frm' } in
            (ac', newAddr)

-- Compute a simple hash function for use within the allocation frames.
-- This doesn't need to be very good, just fast and good enough to 
-- avoid collisions within one allocation frame.
allocNameHash :: Int -> ByteString -> Int
allocNameHash n bs = case BS.uncons bs of
    Nothing -> 101*n
    Just (w8,bs') -> 
        let w8i = fromIntegral w8 in
        let n' = (173 * n) + (83 * w8i) in
        allocNameHash n' bs'

-- add annotation to frame without seek
addToFrame :: Allocation -> AllocFrame -> AllocFrame
addToFrame an frm =
    let list' = Map.insert (alloc_addr an) an (alloc_list frm) in
    frm { alloc_list = list' }

-- add annotation to frame with seek
addToFrameS :: Allocation -> Int -> AllocFrame -> AllocFrame
addToFrameS an hkey frm =
    let list' = Map.insert (alloc_addr an) an (alloc_list frm) in
    let add_an = Just . (an:) . maybe [] id in
    let seek' = IntMap.alter add_an hkey (alloc_seek frm) in
    AllocFrame { alloc_list = list', alloc_seek = seek' }

foreign import ccall unsafe "string.h memcmp" c_memcmp
    :: Ptr Word8 -> Ptr Word8 -> CSize -> IO CInt
