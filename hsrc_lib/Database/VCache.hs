

module Database.VCache
    ( VRef, PVar
    , VCache
    , VCacheStats(..)
    , openVCache
    , module Database.VCache.VCacheable

    -- * Utility
    ) where

import Database.VCache.Types (VRef, PVar, VCache, VCacheStats(..))
import Database.VCache.VCacheable
import Database.VCache.Impl
import Database.VCache.Open

