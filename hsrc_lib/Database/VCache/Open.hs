
module Database.VCache.Open
    ( openVCache
    ) where

import Control.Exception
import System.FileLock (FileLock)
import qualified System.FileLock as FileLock
import qualified System.FilePath as FP
import qualified System.EasyFile as EasyFile
import qualified System.IO.Error as IOE

import Foreign.Storable
import Foreign.Marshal.Alloc
import Foreign.Ptr

import Data.Bits
import Data.IORef
import qualified Data.IntMap as IntMap
import qualified Data.ByteString as BS

import Database.LMDB.Raw
import Database.VCache.Types 
import Database.VCache.RWLock
import Database.VCache.Aligned


-- | Open a VCache with a given database file. 
--
-- In most cases, a Haskell process should only use one instance of VCache
-- for the whole application. This greatly simplifies dataflow between the
-- subprograms. Frameworks, libraries, and plugins that use VCache should
-- receive VCache as an argument, usually a stable subdirectory to provide
-- persistence without namespace collisions. If VCache is opened by a module
-- other than Main, please consider that a design smell.
--
-- > openVCache nMegaBytes filePath
--
-- Developers must choose the database file name. I'd leave 'cache' out of
-- the name unless you want users to feel free to delete it. An additional
-- lockfile is created with the same file name plus the "-lock" suffix. An
-- exception will be thrown if the file cannot be created or opened.
--
-- In addition to the file, developers must choose a maximum database size
-- in megabytes. Some systems may be limited, e.g. due to virtual address
-- space of the CPU (frequently 48 bits even on a 64 bit system) or user
-- quotas. It is not a problem to change this value between runs.
--
openVCache :: Int -> FilePath -> IO VCache
openVCache nMB fp = do
    EasyFile.createDirectoryIfMissing True (FP.takeDirectory fp)
    let fpLock = fp ++ "-lock"
    let nBytes = (max 1 nMB) * 1024 * 1024
    mbLock <- FileLock.tryLockFile fpLock FileLock.Exclusive 
    case mbLock of
        Nothing -> ioError $ IOE.mkIOError
            IOE.alreadyInUseErrorType
            "failure to acquire exclusive file lock"
            Nothing (Just fpLock)
        Just fl -> openVC' nBytes fl fp 
                    `onException` FileLock.unlockFile fl

vcFlags :: [MDB_EnvFlag] 
vcFlags = [MDB_NOSUBDIR     -- open file name, not directory name
          ,MDB_NOSYNC       -- no automatic synchronization
          ,MDB_NOLOCK       -- leave lock management to VCache
          ]

vcRootPath :: BS.ByteString
vcRootPath = BS.empty

-- Default address for allocation. We start this high to help 
-- regulate serialization sizes and simplify debugging.
vcAllocStart :: Address 
vcAllocStart = 9999999


openVC' :: Int -> FileLock -> FilePath -> IO VCache
openVC' nBytes fl fp = do
    dbEnv <- mdb_env_create
    mdb_env_set_mapsize dbEnv nBytes
    mdb_env_set_maxdbs dbEnv 5
    mdb_env_open dbEnv fp vcFlags
    flip onException (mdb_env_close dbEnv) $ do

        -- Initial transaction. 
        txnInit <- mdb_txn_begin dbEnv Nothing False
        dbiMemory <- mdb_dbi_open' txnInit (Just "@") [MDB_CREATE, MDB_INTEGERKEY]
        dbiRoots  <- mdb_dbi_open' txnInit (Just "/") [MDB_CREATE]
        dbiHashes <- mdb_dbi_open' txnInit (Just "#") [MDB_CREATE, MDB_INTEGERKEY, MDB_DUPSORT, MDB_DUPFIXED, MDB_INTEGERDUP]
        dbiRefct  <- mdb_dbi_open' txnInit (Just "^") [MDB_CREATE, MDB_INTEGERKEY]
        dbiRefct0 <- mdb_dbi_open' txnInit (Just "%") [MDB_CREATE, MDB_INTEGERKEY]
        allocEnd <- findLastAddrAllocated txnInit dbiMemory
        mdb_txn_commit txnInit

        let allocStart = nextAllocAddress allocEnd
        rwLock <- newRWLock
        memVRefs <- newIORef IntMap.empty
        memPVars <- newIORef IntMap.empty
        allocator <- newIORef allocStart
        let closeVC = mdb_env_close dbEnv >> FileLock.unlockFile fl
        _ <- mkWeakIORef memVRefs closeVC

        return $! VCache 
            { vcache_path = vcRootPath
            , vcache_space = VSpace 
                { vcache_lockfile = fl
                , vcache_db_env = dbEnv
                , vcache_db_memory = dbiMemory
                , vcache_db_vroots = dbiRoots
                , vcache_db_caddrs = dbiHashes
                , vcache_db_refcts = dbiRefct
                , vcache_db_refct0 = dbiRefct0
                , vcache_db_rwlock = rwLock
                , vcache_mem_vrefs = memVRefs
                , vcache_mem_pvars = memPVars
                , vcache_allocator = allocator
                }
            }

-- our allocator should be set for the next *even* address.
nextAllocAddress :: Address -> Address
nextAllocAddress addr | (0 == (addr .&. 1)) = 2 + addr
                      | otherwise           = 1 + addr

-- Determine the last VCache VRef address allocated, based on the
-- actual database contents. If nothing is
findLastAddrAllocated :: MDB_txn -> MDB_dbi' -> IO Address
findLastAddrAllocated txn dbiMemory = alloca $ \ pKey ->
    mdb_cursor_open' txn dbiMemory >>= \ crs ->
    mdb_cursor_get' MDB_LAST crs pKey nullPtr >>= \ bFound ->
    mdb_cursor_close' crs >>
    if (not bFound) then return vcAllocStart else 
    peek pKey >>= \ key -> 
    let bBadSize = fromIntegral (sizeOf vcAllocStart) /= mv_size key in 
    if bBadSize then fail "VCache memory table corrupted" else
    peekAligned (castPtr (mv_data key)) 

