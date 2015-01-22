

module Database.VCache.Sync
    ( vcacheSync
    ) where

import Control.Concurrent.STM
--import Control.Concurrent.STM.TVar
import Control.Concurrent.MVar
import Database.VCache.Types

-- | If you use a lot of non-durable transactions, you may wish to
-- ensure they are synchronized to disk at various times. vcacheSync
-- will simply wait for all transactions committed up to this point.
-- This is the same as running a durable transaction, except that
-- vcacheSync doesn't require a PVar.
--
-- It is recommended you perform a vcacheSync as part of graceful
-- shutdown of any application that uses VCache.
--
vcacheSync :: VCache -> IO ()
vcacheSync (VCache vs _) = do
    mvWait <- newEmptyMVar
    let onSync = putMVar mvWait ()
    let fauxBatch = VTxBatch onSync []
    let var = vcache_writes vs
    let commitFauxBatch = do
            bs <- readTVar var
            writeTVar var (fauxBatch : bs)
    atomically commitFauxBatch
    takeMVar mvWait
