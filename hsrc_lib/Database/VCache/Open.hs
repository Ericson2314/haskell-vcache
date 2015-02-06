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
import Data.Maybe
import qualified Data.Map.Strict as Map
import qualified Data.ByteString as BS
import qualified Data.List as L
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Concurrent

import qualified System.IO as Sys
import qualified System.Exit as Sys

import Database.LMDB.Raw
import Database.VCache.Types 
import Database.VCache.RWLock
import Database.VCache.Aligned
import Database.VCache.Write -- Writer step
import Database.VCache.CacheClean -- Cache manager



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
-- Note: If errors cause the VCache writer threads to halt, VCache will
-- halt the whole program rather than risk database corruption or wasted
-- work. A likely source of error is storing error values in a PVar.
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
-- I'm providing a non-empty root bytestring. There are a few reasons
-- for this. LMDB doesn't support zero-sized keys. And the empty
-- bytestring will indicate anonymous PVars in the allocator. And if
-- I ever want PVar roots within VCache, I can use a different prefix.
--
-- The maximum path, including the PVar name, is 511 bytes. That is
-- enough for almost any use case, especially since roots should not
-- depend on domain data. Too large a path results in runtime error.
vcRootPath :: BS.ByteString
vcRootPath = BS.singleton 47

-- Default address for allocation. We start this high to help 
-- regulate serialization sizes and simplify debugging.
vcAllocStart :: Address 
vcAllocStart = 999999999

-- Default cache size is somewhat arbitrary. I've chosen to set it
-- to ten megabytes (as documented in the Cache module). 
vcDefaultCacheLimit :: Int
vcDefaultCacheLimit = 10 * 1024 * 1024 

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
        memory <- newMVar (initMemory allocStart)
        tvWrites <- newTVarIO (Writes Map.empty [])
        mvSignal <- newMVar () 
        cLimit <- newIORef vcDefaultCacheLimit
        cSize <- newIORef 0
        ctWrites <- newIORef $ WriteCt 0 0 0
        gcStart <- newIORef Nothing
        gcCount <- newIORef 0
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
                    , vcache_memory = memory
                    , vcache_signal = mvSignal
                    , vcache_writes = tvWrites
                    , vcache_rwlock = rwLock
                    , vcache_climit = cLimit
                    , vcache_csize = cSize
                    , vcache_signal_writes = updWriteCt ctWrites
                    , vcache_ct_writes = ctWrites
                    , vcache_alloc_init = allocStart
                    , vcache_gc_start = gcStart
                    , vcache_gc_count = gcCount
                    }
                }

        initVCacheThreads (vcache_space vc)
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

-- initialize memory based on initial allocation position
initMemory :: Address -> Memory
initMemory addr = m0 where
    af = AllocFrame Map.empty Map.empty addr
    ac = Allocator addr af af af
    gc = GC Map.empty Map.empty
    m0 = Memory Map.empty Map.empty gc ac

-- Update write counts.
updWriteCt :: IORef WriteCt -> Writes -> IO ()
updWriteCt var w = modifyIORef' var $ \ wct ->
    let frmCt = 1 + wct_frames wct in
    let pvCt = wct_pvars wct + Map.size (write_data w) in
    let synCt = wct_sync wct + L.length (write_sync w) in
    WriteCt { wct_frames = frmCt, wct_pvars = pvCt, wct_sync = synCt }


-- | Create background threads needed by VCache.
initVCacheThreads :: VSpace -> IO ()
initVCacheThreads vc = begin where
    begin = do
        task (writerStep vc)
        task (cleanCache vc)
    task step = void (forkIO (forever step `catch` onE))
    onE :: SomeException -> IO ()
    onE e | isBlockedOnMVar e = return () -- full GC of VCache
    onE e = do
        putErrLn "VCache writer thread has failed."
        putErrLn (indent "  " (show e))
        putErrLn "Halting program."
        Sys.exitFailure

isBlockedOnMVar :: (Exception e) => e -> Bool
isBlockedOnMVar = isJust . test . toException where
    test :: SomeException -> Maybe BlockedIndefinitelyOnMVar
    test = fromException

putErrLn :: String -> IO ()
putErrLn = Sys.hPutStrLn Sys.stderr

indent :: String -> String -> String
indent ws = (ws ++) . indent' where
    indent' ('\n':s) = '\n' : ws ++ indent' s
    indent' (c:s) = c : indent' s
    indent' [] = []





