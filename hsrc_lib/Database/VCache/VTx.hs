
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
    -- fast path for read-only, non-durable actions
    let bWrite = not (Map.null (vtx_writes s)) in
    let bSync = vtx_durable s in
    let bDone = not (bWrite || bSync) in
    if bDone then return (return r) else
    -- otherwise, we update shared queue w/ potential conflicts
    readTVar (vcache_writes vc) >>= \ w ->
    let wdata' = updateLog (vtx_writes s) (write_data w) in
    let wsync' = updateSync bSync mvWait (write_sync w) in
    let w' = Writes { write_data = wdata', write_sync = wsync' } in
    writeTVar (vcache_writes vc) w' >>= \ () ->
    return $ do
        w' `seq` signalWriter vc 
        when bSync (takeMVar mvWait)
        return r

-- Signal the writer of work to do.
signalWriter :: VSpace  -> IO ()
signalWriter vc = void (tryPutMVar (vcache_signal vc) ())
{-# INLINE signalWriter #-}

-- Record recent writes for each PVar.
updateLog :: WriteLog -> WriteLog -> WriteLog
updateLog updates writeLog = Map.union updates writeLog 
{-# INLINE updateLog #-}

-- Track which threads are waiting on a commit signal.
updateSync :: Bool -> MVar () -> [MVar ()] -> [MVar ()]
updateSync bSync v = if bSync then (v:) else id
{-# INLINE updateSync #-}

-- | Durability for a VTx transaction is optional: it requires an
-- additional wait for the background thread to signal that it has
-- committed content to the persistence layer. Due to how writes 
-- are batched, a durable transaction may share its wait with many
-- other transactions that occur at more or less the same time.
-- 
-- Developers should mark a transaction durable only if necessary
-- based on domain layer policies. E.g. for a shopping service, 
-- normal updates and views of the virtual shopping cart might not
-- be durable while committing to a purchase is durable. 
--
markDurable :: VTx ()
markDurable = VTx $ modify $ \ vtx -> 
    vtx { vtx_durable = True }
{-# INLINE markDurable #-}

-- | This variation of markDurable makes it easier to short-circuit
-- complex computations to decide durability when the transaction is
-- already durable. If durability is already marked, the boolean is
-- not evaluated.
markDurableIf :: Bool -> VTx ()
markDurableIf b = VTx $ modify $ \ vtx -> 
    let bDurable = vtx_durable vtx || b in
    vtx { vtx_durable = bDurable }
{-# INLINE markDurableIf #-}

