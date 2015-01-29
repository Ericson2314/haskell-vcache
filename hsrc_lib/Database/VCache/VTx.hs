
module Database.VCache.VTx
    ( VTx
    , runVTx
    , liftSTM
    , markDurable
    ) where

import Control.Monad 
import Control.Monad.Trans.State.Strict
import Control.Concurrent.STM
import Control.Concurrent.MVar
import qualified Data.Map.Strict as Map
import Database.VCache.Types

-- | runVTx executes a transaction that may involve both STM TVars
-- (via liftSTM) and VCache PVars (via readPVar, writePVar). 
runVTx :: VSpace -> VTx a -> IO a
runVTx vc action = do
    mvWait <- newEmptyMVar
    (bDurable, result) <- atomically (runVTx' vc mvWait action)
    when bDurable (takeMVar mvWait)
    return result
{-# INLINABLE runVTx #-}

runVTx' :: VSpace -> MVar () -> VTx a -> STM (Bool, a)
runVTx' vc mvWait action = 
    let s0 = VTxState vc Map.empty False in
    runStateT (_vtx action) s0 >>= \ (r,s) ->
    -- early exit for read-only, non-durable actions
    let bWrite = not (Map.null (vtx_writes s)) in
    let bSync = vtx_durable s in
    let bDone = not (bWrite || bSync) in
    if bDone then return (False, r) else
    -- otherwise, we must update the shared queue
    readTVar (vcache_writes vc) >>= \ w ->
    let d' = updateLog (vtx_writes s) (write_data w) in
    let s' = mvWait : write_sync w in
    let w' = Writes { write_data = d', write_sync = s' } in
    writeTVar (vcache_writes vc) w' >>
    return (w' `seq` bSync, r)
{-# NOINLINE runVTx' #-} 

-- I assume the log is larger than recent updates.
-- This will usually be true after a few transactions.
-- Data.Map hedge union favors the big set on the left.
-- For performance, I want a right-biased hedge union.
updateLog :: WriteLog -> WriteLog -> WriteLog
updateLog updates writeLog = Map.unionWith rightBias writeLog updates
{-# INLINE updateLog #-}

rightBias :: a -> b -> b
rightBias _ b = b

-- | A VTx transaction is Atomic, Consistent, and Isolated. Durability
-- is optional, and requires an additional wait for a background writer
-- thread to signal that contents written and read are consistent with 
-- the persistence layer. 
--
-- The decision to mark a transaction durable is at the domain layer.
-- Developers may decide based on the specific variables and values
-- involved, e.g. marking durability when committing to a purchase,
-- but not for merely updating the shopping cart.
--
markDurable :: VTx ()
markDurable = VTx $ modify $ \ vtx -> 
    vtx { vtx_durable = True }
{-# INLINE markDurable #-}

