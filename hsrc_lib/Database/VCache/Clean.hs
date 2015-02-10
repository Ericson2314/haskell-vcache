
-- This module manages the ephemeron tables and VRef caches. 
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


