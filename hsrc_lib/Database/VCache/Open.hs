
module Database.VCache.Open
    ( openVCache
    ) where

import System.FileLock (FileLock)
import qualified System.FileLock as FileLock
import qualified System.FilePath as FP
import qualified System.EasyFile as EasyFile
import qualified System.IO.Error as IOE
import Control.Exception

import Data.IORef
import qualified Data.IntMap as IntMap
import qualified Data.ByteString as BS

import Database.LMDB.Raw
import Database.VCache.Types 
import Database.VCache.RWLock


-- | Open a VCache with a given database file. An additional lockfile
-- will be created using the same name plus a "-lock" suffix. An IO error
-- will be thrown if the cache database could not be opened correctly.
--
-- In most cases, a Haskell process should only use one instance of VCache
-- for the whole application. This greatly simplifies dataflow between the
-- subprograms. Frameworks, libraries, and plugins that use VCache should
-- receive VCache as an argument, usually a stable subdirectory to provide
-- persistence without namespace collisions. If VCache is opened by a module
-- other than Main, please consider that a design smell.
-- 
openVCache :: FilePath -> IO VCache
openVCache fp = do
    EasyFile.createDirectoryIfMissing True (FP.takeDirectory fp)
    let fpLock = fp ++ "-lock"
    mbLock <- FileLock.tryLockFile fpLock FileLock.Exclusive 
    case mbLock of
        Nothing -> ioError $ IOE.mkIOError
            IOE.alreadyInUseErrorType
            "failed to acquire exclusive file lock"
            Nothing (Just fpLock)
        Just fl -> openVC' fl fp `onException` FileLock.unlockFile fl


-- LMDB demands we configure a max size up front, which is a bit irritating.
-- For now, I'll just set the scale to 512 petabytes, or about 1:16th the
-- available address space on a 64-bit system (typically 63-bit address space).
--
-- This should effectively scale to the point you can be sharding applications
-- across multiple disks and some real databases.
vcMaxSize :: Int
vcMaxSize = 2 ^ (59 :: Int) 

vcFlags :: [MDB_EnvFlag] 
vcFlags = [MDB_NOSUBDIR -- open file name, not directory name
          ,MDB_NOSYNC -- no automatic synchronization
          ,MDB_NOLOCK -- leave lock management to VCache
          ]

vcRootPath :: BS.ByteString
vcRootPath = BS.empty

openVC' :: FileLock -> FilePath -> IO VCache
openVC' fl fp = do
    dbEnv <- mdb_env_create
    mdb_env_set_mapsize dbEnv vcMaxSize
    mdb_env_set_maxdbs dbEnv 5
    mdb_env_open dbEnv fp vcFlags

    txnInit <- mdb_txn_begin dbEnv Nothing False
    dbiMemory <- mdb_dbi_open' txnInit Nothing [MDB_CREATE, MDB_INTEGERKEY]
    dbiRoots  <- mdb_dbi_open' txnInit (Just "/") [MDB_CREATE]
    dbiHashes <- mdb_dbi_open' txnInit (Just "#") [MDB_CREATE, MDB_INTEGERKEY, MDB_DUPSORT, MDB_INTEGERDUP]
    dbiRefct  <- mdb_dbi_open' txnInit (Just "^") [MDB_CREATE, MDB_INTEGERKEY]
    dbiRefct0 <- mdb_dbi_open' txnInit (Just "%") [MDB_CREATE, MDB_INTEGERKEY]
    -- todo: acquire next free address
    mdb_txn_commit txnInit

    rwLock <- newRWLock
    memVRefs <- newIORef IntMap.empty
    memPVars <- newIORef IntMap.empty

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
            }
        }
