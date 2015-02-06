
-- This module manages the ephemeron tables, VRef caches, and
-- garbage collection. Basically, it keeps things clean. The
-- provided function is run forever by a thread.
module Database.VCache.Clean
    ( cleanStep
    ) where

import Control.Monad
import qualified Data.Map.Strict as Map
import Control.Concurrent

import Database.LMDB.Raw
import Database.VCache.Types

-- In each step, we must clear ephemeron tables and manage garbage
-- collection. The most important constraint, here, is that we do
-- not hold the vcache_memory MVar for too long. Other than that,
-- we must also have 'halting points' when we have done enough
-- work and we can rest until the writer is busy. Ideally, we can
-- spend most of our time waiting on the vcache_signal MVar if the
-- application itself is idle.
-- 
cleanStep :: VSpace -> IO ()
cleanStep vc = do
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





-- The garbage collection process for VCache is very simple. We 
-- simply scan the zeroes from the database, filter those in the
-- ephemeron tables, and add the rest to our GC target. We'll do
-- this in incremental chunks.
--
-- We must also limit GC based on the pace the writer is setting.
-- If the number of GC items is above some threshold, we'll need
-- to skip the GC step until the writer has cleared the GC frame.
--
-- The GC value in vcache_memory prevents VRefs from resurrection
-- after selection for GC. This uses the same frame-based reader 
-- lock as the allocator; however, unlike allocations, only the
-- cleanStep thread updates the gc_frm_next field.

--runGarbageCollector :: VSpace -> MDB_txn -> Int -> IO WriteBatch
--runGarbageCollector vc txn gcLimit = error "TODO"


