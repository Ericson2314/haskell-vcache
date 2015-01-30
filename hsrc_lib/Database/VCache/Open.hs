module Database.VCache.Open
    ( openVCache
    ) where

import Control.Monad
import Control.Exception
import System.FileLock (FileLock)
import qualified System.FileLock as FileLock
import qualified System.EasyFile as EasyFile
import qualified System.IO.Error as IOE
import Foreign.Storable
import Foreign.Marshal.Alloc
import Foreign.Ptr

import Data.Bits
import Data.IORef
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map
import qualified Data.ByteString as BS
import qualified Data.List as L
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar

import Database.LMDB.Raw
import Database.VCache.Types 
import Database.VCache.RWLock
import Database.VCache.Aligned
import Database.VCache.Write


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
-- lockfile is created with the same file name plus a "-lock" suffix. An
-- exception is raised if the files cannot be created, locked, or opened.
--
-- In addition to the file location, developers must choose a maximum 
-- database size in megabytes. This determines how much address space 
-- is memory mapped, and the maximum file size. Depending on your app,
-- you may need to reserve some address space for other resources or
-- databases. Note: Some 64-bit CPUs limit you to 48 bits address, so
-- you might be limited to allocating ~127 terabytes.
--
-- VCache will generally remain in memory after it opens, i.e. due to
-- background GC and writer threads. Assume that, once you've opened 
-- a VCache instance, you're stuck with it.
--
-- Note: If errors cause the VCache threads to halt prematurely, the
-- whole program will halt. This prevents database corruption and
-- limits wasted effort writing to PVars that cannot be persisted.
--
openVCache :: Int -> FilePath -> IO VCache
openVCache nMB fp = do
    let (fdir,fn) = EasyFile.splitFileName fp
    let eBadFile = fp ++ " not recognized as a file name"
    when (L.null fn) (fail $ "openVCache: " ++ eBadFile)
    EasyFile.createDirectoryIfMissing True fdir
    let fpLock = fp ++ "-lock"
    let nBytes = (max 1 nMB) * 1024 * 1024
    mbLock <- FileLock.tryLockFile fpLock FileLock.Exclusive 
    case mbLock of
        Nothing -> ioError $ IOE.mkIOError
            IOE.alreadyInUseErrorType
            "openVCache lockfile"
            Nothing (Just fpLock)
        Just fl -> openVC' nBytes fl fp 
                    `onException` FileLock.unlockFile fl

vcFlags :: [MDB_EnvFlag] 
vcFlags = [MDB_NOSUBDIR     -- open file name, not directory name
          ,MDB_NOLOCK       -- leave lock management to VCache
          ]

--
-- I'm providing a non-empty root bytestring because it allows
-- me some arbitrary namespaces for VCache internal use, if I
-- ever feel the need to use them. Also, I will use the empty
-- bytestring to indicate anonymous PVars in the Allocation type.
--
-- The maximum path, including the PVar name, is 511 bytes. That
-- should be enough for almost any use case.
vcRootPath :: BS.ByteString
vcRootPath = BS.singleton 47

-- Default address for allocation. We start this high to help 
-- regulate serialization sizes and simplify debugging.
vcAllocStart :: Address 
vcAllocStart = 999999999


openVC' :: Int -> FileLock -> FilePath -> IO VCache
openVC' nBytes fl fp = do
    dbEnv <- mdb_env_create
    mdb_env_set_mapsize dbEnv nBytes
    mdb_env_set_maxdbs dbEnv 5
    mdb_env_open dbEnv fp vcFlags
    flip onException (mdb_env_close dbEnv) $ do

        -- initial transaction to grab database handles and init allocator
        txnInit <- mdb_txn_begin dbEnv Nothing False
        dbiMemory <- mdb_dbi_open' txnInit (Just "@") [MDB_CREATE, MDB_INTEGERKEY]
        dbiRoots  <- mdb_dbi_open' txnInit (Just "/") [MDB_CREATE]
        dbiHashes <- mdb_dbi_open' txnInit (Just "#") [MDB_CREATE, MDB_INTEGERKEY, MDB_DUPSORT, MDB_DUPFIXED, MDB_INTEGERDUP]
        dbiRefct  <- mdb_dbi_open' txnInit (Just "^") [MDB_CREATE, MDB_INTEGERKEY]
        dbiRefct0 <- mdb_dbi_open' txnInit (Just "%") [MDB_CREATE, MDB_INTEGERKEY]
        allocEnd <- findLastAddrAllocated txnInit dbiMemory
        mdb_txn_commit txnInit

        -- ephemeral resources
        let allocStart = nextAllocAddress allocEnd
        let initAllocator = freshAllocator allocStart
        allocator <- newIORef initAllocator
        memVRefs <- newIORef IntMap.empty
        memPVars <- newIORef IntMap.empty
        tvWrites <- newTVarIO (Writes Map.empty [])
        mvSignal <- newMVar () 
        rwLock <- newRWLock

        -- Realistically, we're unlikely GC our VCache before the
        -- process halts. But this should provide some graceful
        -- shutdown behavior.
        let closeVC = do
                mdb_env_close dbEnv
                FileLock.unlockFile fl
        _ <- mkWeakMVar mvSignal closeVC

        let vc = VCache 
                { vcache_path = vcRootPath
                , vcache_space = VSpace 
                    { vcache_lockfile = fl
                    , vcache_db_env = dbEnv
                    , vcache_db_memory = dbiMemory
                    , vcache_db_vroots = dbiRoots
                    , vcache_db_caddrs = dbiHashes
                    , vcache_db_refcts = dbiRefct
                    , vcache_db_refct0 = dbiRefct0
                    , vcache_mem_vrefs = memVRefs
                    , vcache_mem_pvars = memPVars
                    , vcache_allocator = allocator
                    , vcache_signal = mvSignal
                    , vcache_writes = tvWrites
                    , vcache_rwlock = rwLock
                    }
                }

        -- Star the background writer threads. In addition to performing
        -- writes, these threads must handle ongoing GC tasks.
        initWriterThreads (vcache_space vc)
        return $! vc

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

freshAllocator :: Address -> Allocator
freshAllocator addr =
    let f0 = AllocFrame Map.empty IntMap.empty addr in
    Allocator addr f0 f0 f0

