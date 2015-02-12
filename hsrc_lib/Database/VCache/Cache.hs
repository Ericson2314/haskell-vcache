

-- | Limited cache control.
module Database.VCache.Cache
    ( setVRefsCacheSize
    , clearVRefsCache
    , clearVRefCache
    ) where

import Control.Applicative ((<$>))
import Data.IORef
import Control.Concurrent.MVar
import qualified Data.Map.Strict as Map
import qualified System.Mem.Weak as Weak
import Database.VCache.Types

-- | VCache uses simple heuristics to decide which VRef contents to
-- hold in memory. One heuristic is a target cache size, which is 
-- set globally for the whole VCache instance. When cache approaches
-- this soft limit, a cleanup function will gradually try to clear
-- items. When the cache runs above, the cleanup function gets more
-- aggressive. Tuning the cache allows developers to influence the
-- memory overhead for using large numbers of VRefs.
--
-- Only VRefs are cached. To cache a PVar, you must hide the PVar or
-- the bulk of its content behind a VRef. 
--
-- By default, the heuristic limit is ten megabytes. Developers are
-- free to tune or reconfigure this limit at any time. The value is
-- specified in bytes. This is a soft limit, i.e. contents will be
-- cleared as the limit is approached, and more aggressively when we 
-- run above the limit.
--
-- Notes: This cache is independent from the page caching the OS will
-- perform for the memory mapped LMDB file. When parses are cheap and
-- structure sharing at the Haskell layer is irrelevant, performance
-- may benefit from using vref' and deref' and relying upon the OS 
-- layer page cache.
--
-- VCache doesn't know sizes at the Haskell layer. Size is estimated
-- from storage costs, but doesn't account for memory amplification
-- (e.g. where a UTF-8 string in VCache is amplified by a factor of
-- twenty to thirty as a list of characters in Haskell). 
--
setVRefsCacheSize :: VSpace -> Int -> IO ()
setVRefsCacheSize = writeIORef . vcache_climit
{-# INLINE setVRefsCacheSize #-}

-- | clearVRefsCache will iterate over all VRefs in Haskell memory at 
-- the time of the call, clearing the cache for each of them. This 
-- operation isn't recommended for common use, as it is very hostile
-- to multiple independent libraries working with VCache. But it may 
-- prove useful for benchmarks or staged applications or similar.
clearVRefsCache :: VSpace -> IO ()
clearVRefsCache vc = do
    ephMap <- mem_vrefs <$> readMVar (vcache_memory vc)
    mapM_ (mapM_ clearVREphCache . Map.elems) (Map.elems ephMap)
{-# INLINABLE clearVRefsCache #-}

clearVREphCache :: VREph -> IO ()
clearVREphCache (VREph { vreph_cache = wc }) =   
    Weak.deRefWeak wc >>= \ mbCache ->
    case mbCache of
        Nothing -> return ()
        Just cache -> writeIORef cache NotCached


-- | Immediately clear the cache associated with a VRef, allowing 
-- any contained data to be GC'd. Normally, VRef cached values are
-- cleared either by a background thread (cf. setVRefsCacheSize) or
-- when the VRef itself is garbage collected from Haskell memory.
-- But sometimes the programmer knows better.
clearVRefCache :: VRef a -> IO ()
clearVRefCache v = writeIORef (vref_cache v) NotCached
{-# INLINE clearVRefCache #-}


