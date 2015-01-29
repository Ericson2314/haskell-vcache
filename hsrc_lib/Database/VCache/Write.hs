
-- Implementation of the Writer threads.
module Database.VCache.Write
    ( initWriterThreads
    ) where


import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.MVar
import Database.VCache.Types

-- VCache actually needs


initWriterThreads :: VSpace -> IO ()
initWriterThreads _vc = return ()