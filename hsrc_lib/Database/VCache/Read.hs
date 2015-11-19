
module Database.VCache.Read
    ( readAddrIO
    , readRefctIO
    , withBytesIO
    ) where

import Control.Monad
import qualified Data.Map.Strict as Map
import Control.Concurrent.MVar
import Data.Word
import Foreign.Ptr
import Foreign.Storable
import Foreign.Marshal.Alloc

import Database.LMDB.Raw

import Database.VCache.Types
import Database.VCache.VGetInit
import Database.VCache.VGetAux
import Database.VCache.Refct

-- | Parse contents at a given address. Returns both the value and the
-- cache weight, or fails. This first tries reading the database, then
-- falls back to reading from recent allocation frames. If the address
-- does not have defined data at either location, this will fail.
readAddrIO :: VSpace -> Address -> VGet a -> IO (a, Int)
readAddrIO vc addr = withAddrValIO vc addr . readVal vc
{-# INLINE readAddrIO #-}

withAddrValIO :: VSpace -> Address -> (MDB_val -> IO a) -> IO a
withAddrValIO vc addr action = 
    alloca $ \ pAddr ->
    poke pAddr addr >>
    let vAddr = MDB_val { mv_data = castPtr pAddr
                        , mv_size = fromIntegral (sizeOf addr)
                        }
    in
    withRdOnlyTxn vc $ \ txn -> 
    mdb_get' txn (vcache_db_memory vc) vAddr >>= \ mbData ->
    case mbData of
        Just vData -> action vData -- found data in database (ideal)
        Nothing -> -- since not in the database, try the allocator
            let ff = Map.lookup addr . alloc_list in
            readMVar (vcache_memory vc) >>= \ memory ->
            let ac = mem_alloc memory in
            case allocFrameSearch ff ac of
                Just _data -> withByteStringVal _data action -- found data in allocator
                Nothing -> fail $ "VCache: address " ++ show addr ++ " is undefined!"
{-# NOINLINE withAddrValIO #-}

readVal :: VSpace -> VGet a -> MDB_val -> IO (a, Int)
readVal vc p v = _vget (vgetFull p) s0 >>= retv where
    size = fromIntegral (mv_size v)
    s0 = VGetS { vget_children = []
               , vget_target = mv_data v
               , vget_limit = mv_data v `plusPtr` size
               , vget_space = vc
               }
    retv (VGetR result _) = return (result, size)
    retv (VGetE eMsg) = fail eMsg

-- vget with initializer for dependencies.
-- asserts that we parse all available input.
vgetFull :: VGet a -> VGet a
vgetFull parse = do
    vgetInit 
    r <- parse
    assertDone
    return r

assertDone :: VGet ()
assertDone = isEmpty >>= \ b -> unless b (fail emsg) where
    emsg = "VCache: failed to read full input" 
{-# INLINE assertDone #-}


-- | Read a reference count for a given address. 
readRefctIO :: VSpace -> Address -> IO Int
readRefctIO vc addr = 
    alloca $ \ pAddr ->
    withRdOnlyTxn vc $ \ txn -> 
    poke pAddr addr >>
    let vAddr = MDB_val { mv_data = castPtr pAddr
                        , mv_size = fromIntegral (sizeOf addr) }
    in
    mdb_get' txn (vcache_db_refcts vc) vAddr >>= \ mbData ->
    maybe (return 0) readRefctBytes mbData

-- | Zero-copy access to raw bytes for an address.
withBytesIO :: VSpace -> Address -> (Ptr Word8 -> Int -> IO a) -> IO a
withBytesIO vc addr action = 
    withAddrValIO vc addr $ \ v -> 
    action (mv_data v) (fromIntegral (mv_size v))
{-# INLINE withBytesIO #-}




