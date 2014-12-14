

module Database.VCache
    ( VRef, VCache, VCacheable(..) 

    -- * Serialization
    , VPut, VGet

    -- * Miscellaneous
    , vref_origin
    ) where

import Database.VCache.Types
