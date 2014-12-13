

module Database.VCache.Types
    ( VRef(..)
    , Address
    ) where

import Data.Word (Word64)
import Data.IORef
import Data.Function (on)
import Data.Unique

type Address = Word64
type ValHash = Word64

-- | A VRef (value reference) is an opaque pointer to an immutable
-- value backed by an auxiliary address space, the VCache. 
--
-- Holding a VRef will prevent garbage collection of the value.
-- 
-- VRefs may be compared for equality, and are hashable. The hash
-- value for a VRef may change whenever the process is restarted,
-- so should only be used for hashmaps or hashtables.
--
data VRef a = VRef 
    { vref_addr  :: {-# UNPACK #-} !Address -- address within the cache
    , vref_cache :: !VCache                 -- cache holding this VRef
    , vref_weak  :: !(IORef ())             -- to track ephemeral roots
    } 

-- | A VCache models an auxiliary address space for values, where
-- values are more readily persisted and do not burden the Haskell
-- garbage collector.
--
-- Note that VCache doesn't need to be used for persistence. If a
-- Haskell process simply has need for very large values, VCache
-- may be a good option for that. VCache doesn't flush content to
-- disk unless asked. 
-- 
data VCache = VCache
    { vcache_id :: {-# UNPACK #-} !Unique
    }



instance Eq (VRef a) where
    (==) a b = ((==) `on` vref_addr) a b 
            && ((==) `on` (vcache_id . vref_cache)) a b


