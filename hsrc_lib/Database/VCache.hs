

module Database.VCache
    ( VRef, PVar
    , VCache
    , module Database.VCache.VCacheable

    -- * Utility
    , mvref
    ) where

import Database.VCache.Types (VRef, PVar, VCache)
import Database.VCache.Impl (mvref)
import Database.VCache.VCacheable




