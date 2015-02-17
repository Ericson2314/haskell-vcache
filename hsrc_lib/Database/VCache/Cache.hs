{-# LANGUAGE BangPatterns #-}
-- | Limited cache control.
module Database.VCache.Cache
    ( setVRefsCacheLimit
    , clearVRefsCache
    , clearVRefCache
    ) where

import Data.IORef
import Control.Concurrent.MVar
import qualified Data.Map.Strict as Map
import qualified System.Mem.Weak as Weak
import Database.VCache.Types

-- | VCache uses simple heuristics to decide which VRef contents to
-- hold in memory. One heuristic is a target cache size. Developers
-- may tune this to influence how many VRefs are kept in memory. 
--
-- The value is specified in bytes, and the default is ten megabytes.
--
-- VCache size estimates are imprecise, converging on approximate 
-- size, albeit not accounting for memory amplification (e.g. from a
-- compact UTF-8 string to Haskell's representation for [Char]). The
-- limit given here is soft, influencing how aggressively content is
-- removed from cache - i.e. there is no hard limit on content held
-- by the cache. Estimated cache size is observable via vcacheStats.
--
-- If developers need precise control over caching, they should use
-- normal means to reason about GC of values in Haskell (i.e. VRef is
-- cleared from cache upon GC). Or use vref' and deref' to avoid 
-- caching and use VCache as a simple serialization layer.
-- 
setVRefsCacheLimit :: VSpace -> Int -> IO ()
setVRefsCacheLimit vc !n = writeIORef (vcache_climit vc) n
{-# INLINE setVRefsCacheLimit #-}

-- | clearVRefsCache will iterate over cached VRefs in Haskell memory 
-- at the time of the call, clearing the cache for each of them. This 
-- operation isn't recommended for common use. It is rather hostile to
-- independent libraries working with VCache. But this function may 
-- find some use for benchmarks or staged applications.
clearVRefsCache :: VSpace -> IO ()
clearVRefsCache vc = do 
    cvrefs <- swapMVar (vcache_cvrefs vc) Map.empty
    mapM_ (mapM_ clearVREphCache . Map.elems) (Map.elems cvrefs)

clearVREphCache :: VREph -> IO ()
clearVREphCache (VREph { vreph_cache = wc }) =   
    Weak.deRefWeak wc >>= \ mbCache ->
    case mbCache of
        Nothing -> return ()
        Just cache -> writeIORef cache NotCached

-- | Immediately clear the cache associated with a VRef, allowing 
-- any contained data to be GC'd. Normally, VRef cached values are
-- cleared either by a background thread or when the VRef itself
-- is garbage collected from Haskell memory. But sometimes the
-- programmer knows best.
clearVRefCache :: VRef a -> IO ()
clearVRefCache v = do
    let vc = vref_space v 
    modifyMVarMasked_ (vcache_cvrefs vc) $ \ cvrefs -> do
        case takeVREph (vref_addr v) (vref_type v) cvrefs of
            Nothing -> return cvrefs -- was not cached
            Just ( _ , cvrefs') -> return cvrefs'
    writeIORef (vref_cache v) NotCached
{-# NOINLINE clearVRefCache #-}


