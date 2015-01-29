
-- Implementation of the Writer threads.
module Database.VCache.Write
    ( initWriterThreads
    ) where

import Control.Monad
import Control.Exception
import Control.Concurrent
import Control.Concurrent.STM
import qualified Data.Map.Strict as Map
import qualified Data.List as L
import Data.Maybe (isNothing)

import qualified System.IO as Sys
import qualified System.Exit as Sys

import Database.LMDB.Raw
import Database.VCache.Types
import Database.VCache.VPutFini


initWriterThreads :: VSpace -> IO ()
initWriterThreads vc = begin where
    begin = do 
        task (prepareWBatch vc) 
        task (writerStep vc)
    task step = void (forkIO (forever step `catch` onE))
    onE :: SomeException -> IO ()
    onE e = do
        putErrLn "A VCache writer thread has failed."
        putErrLn (indent "  " (show e))
        putErrLn "Halting program."
        mdb_env_sync_flush (vcache_db_env vc) -- keep what we can
        Sys.exitFailure

putErrLn :: String -> IO ()
putErrLn = Sys.hPutStrLn Sys.stderr

indent :: String -> String -> String
indent ws = (ws ++) . indent' where
    indent' ('\n':s) = '\n' : ws ++ indent' s
    indent' (c:s) = c : indent' s
    indent' [] = []


-- Prepare one write batch. This function will wait until space for
-- a new batch is available before preparing a new one, but will
-- begin preparing a new one before the previous batch is fully
-- written and committed to disk. 
--
-- Performance wise, this is advantageous if we have more than one
-- 'capability' and we're very busy. But the main reason for the
-- separate thread is because I don't want to be lazily allocating
-- VRefs and so on that I might need to write in the same pass for
-- consistency. This separation simplifies reasoning.
prepareWBatch :: VSpace -> IO ()
prepareWBatch vc = do
    w <- atomically (takeWrites vc)
    let wLog = Map.toAscList (write_data w) 
    wbList <- mapM (writeWBI vc) wLog
    let wbSync = write_sync w
    let wb = WBatch { wb_list = wbList, wb_sync = wbSync }
    atomically (putWBatch vc wb)
    void (tryPutMVar (vcache_signal vc) ())

writeWBI :: VSpace -> (Address, TxW) -> IO WBI
writeWBI vc (addr, TxW pvar val) = 
    runVPutIO vc (pvar_write pvar val) >>= \ ((), _data, _deps) ->
    return $! WBI 
        { wbi_addr = addr
        , wbi_data = _data
        , wbi_deps = PutChild (Left pvar) : _deps
        }

-- prepareBatch will wait until two conditions are met:
--  (a) there is space for a new batch.
--  (b) at least one write is available.
takeWrites :: VSpace -> STM Writes
takeWrites vc = do
    mb <- readTVar (vcache_wbatch vc)
    unless (isNothing mb) retry
    w <- readTVar (vcache_writes vc)
    when (nullWrites w) retry
    writeTVar (vcache_writes vc) (Writes Map.empty [])
    return w

nullWrites :: Writes -> Bool
nullWrites w = L.null (write_sync w) 
            && Map.null (write_data w)

-- Add a batch for the main writer thread to perform.
putWBatch :: VSpace -> WBatch -> STM ()
putWBatch vc wb = do
    mb <- readTVar (vcache_wbatch vc) 
    unless (isNothing mb) (fail "VCache error: ghost writer") -- shouldn't happen
    writeTVar (vcache_wbatch vc) (Just wb) 

-- Single step for the main VCache writer.
writerStep :: VSpace -> IO ()
writerStep vc = do
    takeMVar (vcache_signal vc)
    putStrLn "VCache: should be writing something"
    


