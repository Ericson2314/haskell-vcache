
module Database.VCache.Read
    ( readAddrIO
    ) where

import Control.Monad
import System.IO.Unsafe (unsafePerformIO)
import Database.VCache.Types


-- | Parse contents at a given address. Returns both the value
-- and the cache weight, or fails.
readAddrIO :: VSpace -> Address -> VGet a -> IO (a, Int)
readAddrIO vc addr parser = fail "todo: readAddrIO"


