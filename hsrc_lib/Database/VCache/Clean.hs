
-- Functions to clear cached values and perform basic garbage collection.
-- I.e. this is the janitorial module.
module Database.VCache.Clean
    ( cleanCache
    , runGarbageCollector
    ) where

import Control.Monad
import qualified Data.Map.Strict as Map
import Control.Concurrent
import Control.Concurrent.STM

import Database.LMDB.Raw
import Database.VCache.Types

-- | VCache keeps two ephemeron tables: one for VRefs and another for
-- PVars. Periodically, we must process these tables to clean up after
-- VRefs or PVars that are no longer in scope. Further, each VRef has
-- a cache that might need to be cleared.
--
-- This operation isn't latency critical, but I do wish to avoid any
-- pauses in the VRef or PVar allocators that might be caused by 
-- managing these maps.
--
-- Special constraints:
-- 
--  * stop on vcache_signal if there isn't enough to do
--  * signal the writer to run GC or clear allocators
--  * limit frequency, though potentially adaptive to
--    rate at which objects are added?
--
cleanCache :: VSpace -> IO ()
cleanCache vc = do
    usleep 42000
    return ()

-- sleep for a number of microseconds
usleep :: Int -> IO ()
usleep = threadDelay
{-# INLINE usleep #-}

-- Alternative usleep:
{- 
usleep :: Int -> IO ()
usleep n = do
    rd <- registerDelay n
    atomically $ do
        b <- readTVar rd
        unless b retry
{-# INLINE usleep #-}
-}
-- I've had some bad experiences with threadDelay causing space leaks. 
-- But maybe it has been fixed? I'll need to check it out later.


runGarbageCollector :: VSpace -> MDB_txn -> Address -> IO ()
runGarbageCollector vc txn allocInit =
    putStrLn "VCache TODO: garbage collection"

