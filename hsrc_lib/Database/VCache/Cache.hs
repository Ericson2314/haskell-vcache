

-- | Limited cache control.
module Database.VCache.Cache
    ( setVCacheSize
    , clearVCache
    ) where

import Control.Applicative ((<$>))
import Data.IORef
import Control.Concurrent.MVar
import qualified Data.Map.Strict as Map
import qualified System.Mem.Weak as Weak
import Database.VCache.Types

-- | VCache uses simple heuristics to decide which VRef contents to
-- hold in memory. One heuristic is a target cache size, which is 
-- set globally for the whole VCache instance. While the cache is
-- larger than the target, a background thread will try to clear
-- cached elements to prevent the Haskell heap from growing too 
-- much.
--
-- VCache doesn't know actual sizes at the Haskell layer. Size is 
-- estimated based on storage costs and recorded as a power of two.
-- The estimated size may be far off in some cases, such as strings
-- where there is considerable memory overhead for list nodes.
--
-- By default, the heuristic limit is ten megabytes. Developers are
-- free to tune or reconfigure this limit at any time. The value is
-- specified in bytes.
--
-- Note: this cache is independent from the page caching the OS will
-- perform for the memory mapped LMDB file. If parses are cheap and
-- structure sharing is irrelevant, developers may prefer to bypass 
-- VRef layer caching, i.e. using vref' and deref', and instead rely 
-- on just the page cache.
--
setVCacheSize :: VSpace -> Int -> IO ()
setVCacheSize = writeIORef . vcache_climit
{-# INLINE setVCacheSize #-}

-- | clearVCache will iterate over the VRefs in Haskell memory at 
-- the time of the call, and clear the cache for each of them.
--
-- This operation isn't recommended for common use, since it can
-- interfere with different subprograms and threads. But it may
-- be useful for some applications.
clearVCache :: VSpace -> IO ()
clearVCache vc = do
    ephMap <- mem_vrefs <$> readMVar (vcache_memory vc)
    mapM_ (mapM_ clearVREphCache . Map.elems) (Map.elems ephMap)
{-# INLINABLE clearVCache #-}

clearVREphCache :: VREph -> IO ()
clearVREphCache (VREph { vreph_cache = wc }) =   
    Weak.deRefWeak wc >>= \ mbCache ->
    case mbCache of
        Nothing -> return ()
        Just cache -> writeIORef cache NotCached

