
-- allocators for PVars and VRefs
module Database.VCache.Alloc
    ( loadRootPVar
    , loadRootPVarIO
    , newPVar
    , newPVarIO
    , newVRefIO
    ) where

-- import Control.Exception
-- import Control.Applicative
-- import Control.Monad
import Data.IORef
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
-- import qualified Data.ByteString.Internal as BSI
import qualified Data.List as L
-- import Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap
import Foreign.Ptr
-- import Foreign.ForeignPtr
import Foreign.Storable
-- import Control.Concurrent.STM
import GHC.Conc (unsafeIOToSTM)
import System.IO.Unsafe (unsafePerformIO)

import Database.LMDB.Raw

import Database.VCache.Types
import Database.VCache.FromAddr
-- import Database.VCache.RWLock
import Database.VCache.Path
import Database.VCache.Aligned
import Database.VCache.VPutFini
-- import Database.VCache.VCacheable
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
allocNamedPVar name _data _deps ac =
    let hkey = allocNameHash 1 name in
    let match an = isPVarAddr (alloc_addr an) && (name == (alloc_name an)) in
    let ff frm = IntMap.lookup hkey (alloc_seek frm) >>= L.find match in
    case allocFrameSearch ff ac of
        Just an -> (ac, alloc_addr an) -- allocated very recently
        Nothing ->
            let newAddr = alloc_new_addr ac in
            let an = Allocation { alloc_name = name
                                , alloc_data = _data
                                , alloc_deps = _deps
                                , alloc_addr = 1 + newAddr
                                }
            in  
            let frm = alloc_frm_next ac in
            let list' = an : alloc_list frm in
            let add_an = Just . (an:) . maybe [] id in
            let seek' = IntMap.alter add_an hkey (alloc_seek frm) in
            let frm' = AllocFrame list' seek' in
            let ac' = ac { alloc_new_addr = 2 + newAddr
                         , alloc_frm_next = frm'
                         }
            in (ac', alloc_addr an)

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
    let frm = alloc_frm_next ac in
    let list' = an : alloc_list frm in
    let frm' = frm { alloc_list = list' } in
    let ac' = ac { alloc_new_addr = 2 + newAddr
                 , alloc_frm_next = frm' 
                 }
    in
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
-- to the normal write process, as part of the VTx context.
allocPVarVTx :: Allocator -> (Allocator, Address)
allocPVarVTx ac = 
    let newAddr = alloc_new_addr ac in
    let pvAddr = 1 + newAddr in
    let ac' = ac { alloc_new_addr = 2 + newAddr } in
    (ac', pvAddr)
   
-- | Construct a VRef in the IO monad. Note that constructing a VRef
-- is relatively expensive: it always requires serializing the value
-- and hashing the result (due to structure sharing). VRefs are read
-- optimized, with the assumption that you'll read them, on average,
-- more frequently than you construct them.
--
-- VRefs of multiple different types can potentially share the same
-- address, assuming the same serialized form.
newVRefIO :: (VCacheable a) => VSpace -> a -> IO (VRef a)
newVRefIO vc v = 
    runVPutIO vc (put v) >>= \ ((), _data, _deps) ->
    let hashBytes = hash _data in
    let nameHash = allocNameHash 1 hashBytes in
    hashBytes `seq` -- compute before locking
    error "TODO: newVRefIO'"
    -- need to obtain a read lock, read the database for any matches on
    -- the hash function and see if one of those addresses is good, and
    -- then check the allocator in case the same VRef has been recently
    -- allocated.

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

