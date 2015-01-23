
-- allocators for PVars and VRefs
module Database.VCache.Alloc
    ( loadRootPVar, loadRootPVarIO
    , newPVarIO, newPVar
    , newPVarIO', newPVar'
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
import System.IO.Unsafe (unsafePerformIO, unsafeInterleaveIO)

import Database.LMDB.Raw

import Database.VCache.Types
import Database.VCache.FromAddr
-- import Database.VCache.RWLock
import Database.VCache.Path
import Database.VCache.Aligned
import Database.VCache.VPutFini
-- import Database.VCache.VCacheable

-- | load a root PVar immediately, in the IO monad. This can be more
-- convenient than loadRootPVar if you need to control where errors
-- are detected or initialization is performed. See loadRootPVar.
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
-- PVar name.
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
-- Semantically, this is a pure operation. If you load with the same 
-- arguments and data many times, you'll receive the same PVar each 
-- time. However, loadRootPVarIO may be preferable to control when and
-- where errors are detected.
--
-- Note: there is no way to delete a root PVar. An application should
-- use only a fixed count of root PVars, usually just one or a few,
-- and push most domain modeling into the data types.
loadRootPVar :: (VCacheable a) => VCache -> ByteString -> a -> PVar a
loadRootPVar vc name ini = unsafePerformIO (loadRootPVarIO vc name ini)
{-# INLINE loadRootPVar #-}

-- | Create a new, unique, anonymous PVar via the IO monad. This is
-- more or less equivalent to newTVarIO, except that the PVar may be
-- referenced from persistent values in the associated VCache. Note
-- that a PVar may be garbage collected if not used.
--
-- Unlike newTVarIO, there is no use case for newPVarIO to construct 
-- 'global' variables. Use loadRootPVar instead, to model globals 
-- that persist from one run of the application to another.
--
-- In most cases, you should favor the transactional newPVar.
--
newPVarIO :: (VCacheable a) => VCache -> a -> IO (PVar a)
newPVarIO = newPVarIO' . vcache_space
{-# INLINE newPVarIO #-}

-- | newPVarIO on VSpace instead of VCache
newPVarIO' :: (VCacheable a) => VSpace -> a -> IO (PVar a)
newPVarIO' vc ini = unsafeInterleaveIO $
    runVPutIO vc (put ini) >>= \ ((), _data, _deps) ->
    atomicModifyIORef (vcache_allocator vc) (allocAnonPVar _data _deps) >>= \ addr ->
    addr2pvar_new vc addr ini
{-# INLINE newPVarIO' #-}

-- allocate PVar such that its initial value will be stored by the background
-- thread. This variation assumes an anonymous PVar.
allocAnonPVar :: ByteString -> [PutChild] -> Allocator -> (Allocator, Address)
allocAnonPVar _data _deps ac =
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
-- This transactional variation has some advantages. The initial value
-- is treated as a normal write, with respect to the backing file. So
-- nothing at all is written if the transaction fails, and writes may
-- be batched if the writer is busy.
--
-- newPVar does consume an address even if the transaction fails. 
newPVar :: (VCacheable a) => VCache -> a -> VTx (PVar a)
newPVar = newPVar' . vcache_space
{-# INLINE newPVar #-}

newPVar' :: (VCacheable a) => VSpace -> a -> VTx (PVar a)
newPVar' vc ini = do
    pvar <- liftSTM $ unsafeIOToSTM $ newPVarVTxIO vc ini
    markForWrite pvar
    return pvar

-- mark a PVar as having been written, such that after the
-- transaction succeeds we'll push it to the backing file.
markForWrite :: PVar a -> VTx ()
markForWrite = error "todo: markForWrite"

newPVarVTxIO :: (VCacheable a) => VSpace -> a -> IO (PVar a)
newPVarVTxIO vc ini = unsafeInterleaveIO $
    atomicModifyIORef (vcache_allocator vc) allocPVarVTx >>= \ addr ->
    addr2pvar_new vc addr ini

-- Allocate just the new PVar address. Do not store any information
-- about this PVar in the allocator. Writing the PVar will be delayed
-- to the normal write process, as part of the VTx context.
allocPVarVTx :: Allocator -> (Allocator, Address)
allocPVarVTx ac = 
    let newAddr = alloc_new_addr ac in
    let pvAddr = 1 + newAddr in
    let ac' = ac { alloc_new_addr = 2 + newAddr } in
    (ac', pvAddr)
    

newVRefIO :: (VCacheable a) => VCache -> a -> IO (VRef a)
newVRefIO = error "todo"

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

