

module Database.VCache.Sync
    ( vcacheSync
    ) where

import Database.VCache.Types
import Database.VCache.VTx

-- | If you use a lot of non-durable transactions, you may wish to
-- ensure they are synchronized to disk at various times. vcacheSync
-- will simply wait for all transactions committed up to this point.
-- This is equivalent to running a durable, read-only transaction.
--
-- It is recommended you perform a vcacheSync as part of graceful
-- shutdown of any application that uses VCache.
--
vcacheSync :: VSpace -> IO ()
vcacheSync vc = runVTx vc markDurable
