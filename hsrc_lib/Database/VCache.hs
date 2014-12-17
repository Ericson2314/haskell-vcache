

module Database.VCache
    ( VRef, PVar
    , VCache

    , Cacheable(..) 
    , VGet, VPut

    -- * Serialization
    -- , VPut, VGet

    -- * Persistence

    -- * Miscellaneous
    -- , region
    module Database.VCache.Cacheable
    ) where

import Database.VCache.Types
import Database.VCache.Cacheable
