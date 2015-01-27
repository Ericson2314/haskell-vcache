
module Database.VCache.VTx
    ( VTx
    , runVTx
    , liftSTM
    , markDurable
    ) where

import Control.Monad 
import Control.Monad.Trans.State
import Control.Concurrent.STM
import Control.Concurrent.MVar
import Database.VCache.Types

-- | runVTx executes a transaction that may involve both STM TVars
-- (via liftSTM) and VCache PVars (via readPVar, writePVar). 
runVTx :: VSpace -> VTx a -> IO a
runVTx vc action = do
    mvWait <- newEmptyMVar
    let onSync = putMVar mvWait ()
    (bDurable, result) <- atomically $ do
        (r,s) <- runStateT (_vtx action) (VTxState vc [] False)
        let b = VTxBatch onSync (vtx_writes s)
        bs <- readTVar (vcache_writes vc)
        writeTVar (vcache_writes vc) (b:bs)
        return (vtx_durable s, r)
    when bDurable (takeMVar mvWait)
    return result

-- | A VTx transaction is Atomic, Consistent, and Isolated. Durability
-- is optional, and requires an additional wait for a background writer
-- thread to signal that contents read or written are consistent with 
-- the persistence layer. 
--
-- Durability is indicated within the transaction. This allows domain
-- layer decisions about whether a transaction should be durable. E.g.
-- we might mark a transaction durable when committing to a purchase,
-- but not for merely updating the shopping cart.
markDurable :: VTx ()
markDurable = VTx $ modify $ \ vtx -> 
    vtx { vtx_durable = True }
{-# INLINE markDurable #-}


