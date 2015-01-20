

module Database.VCache
    ( VRef, PVar
    , VCache
    , openVCache
    , module Database.VCache.VCacheable
    , module Database.VCache.Stats

    -- * Utility
    ) where

import Database.VCache.Types (VRef, PVar, VCache)
import Database.VCache.VCacheable
--import Database.VCache.Impl
import Database.VCache.Open
import Database.VCache.Stats

