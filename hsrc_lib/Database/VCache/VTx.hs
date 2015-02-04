
module Database.VCache.VTx
    ( VTx
    , runVTx
    , liftSTM
    , markDurable
    , markDurableIf
    , getVTxSpace
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
    join (atomically (runVTx' vc mvWait action))
{-# INLINABLE runVTx #-}

runVTx' :: VSpace -> MVar () -> VTx a -> STM (IO a)
runVTx' vc mvWait action = 
    let s0 = VTxState vc Map.empty False in
    runStateT (_vtx action) s0 >>= \ (r,s) ->
    -- early exit for read-only, non-durable actions
    let bWrite = not (Map.null (vtx_writes s)) in
    let bSync = vtx_durable s in
    let bDone = not (bWrite || bSync) in
    if bDone then return (return r) else
    -- otherwise, we must update the shared queue
    readTVar (vcache_writes vc) >>= \ w ->
    let wdata' = updateLog (vtx_writes s) (write_data w) in
    let wsync' = updateSync bSync mvWait (write_sync w) in
    let w' = Writes { write_data = wdata', write_sync = wsync' } in
    writeTVar (vcache_writes vc) w' >>= \ () ->
    return $ w' `seq` do
        signalWriter vc
        when bSync (takeMVar mvWait)
        return r

-- signal the writer thread of new work to do
signalWriter :: VSpace -> IO ()
signalWriter vc = void (tryPutMVar (vcache_signal vc) ())
{-# INLINE signalWriter #-}

-- keep the most recent writes for each PVar, allowing older
-- data to be GC'd.
updateLog :: WriteLog -> WriteLog -> WriteLog
updateLog updates writeLog = Map.union updates writeLog 
{-# INLINE updateLog #-}

updateSync :: Bool -> MVar () -> [MVar ()] -> [MVar ()]
updateSync bSync v = if bSync then (v:) else id
{-# INLINE updateSync #-}

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

-- | This variation of markDurable makes it easier to short-circuit
-- complex computations to decide durability. `markDurableIf False`
-- does not affect durability. If durability is already marked, the
-- boolean is not evaluated.
markDurableIf :: Bool -> VTx ()
markDurableIf b = VTx $ modify $ \ vtx -> 
    let bDurable = vtx_durable vtx || b in
    vtx { vtx_durable = bDurable }
{-# INLINE markDurableIf #-}

