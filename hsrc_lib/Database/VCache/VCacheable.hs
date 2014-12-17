
module Database.VCache.VCacheable
    ( VCacheable(..), VPut, VGet
    ) where

import Data.Typeable
import Database.VCache.Types

-- | To be utilized with VCache, a value must be serializable as a 
-- simple sequence of binary data and VRefs. Stashing then caching 
-- a value must result in an equivalent value. Values are Typeable
-- to support the memory caching features.
-- 
-- Under the hood, structued data is serialized as a pair:
--
--    (ByteString,[VRef])
--
-- Bytes and VRefs must be loaded in the same order and quantity as
-- they were emitted. There is no risk of reading VRef addresses as
-- binary data. This structure greatly simplifies garbage collection.
-- For sums, the bytes should carry the condition information and 
-- determindicate when to load a VRef.
--
-- If you are familiar with Data.Binary or Data.Cereal, the VCacheable
-- interface is modeled off of those, albeit adding the ability to
-- easily reference other values, and lightly optimized for VCache.
-- 
class (Typeable a) => VCacheable a where 
    -- | Stash is a function that will record the value into the VCache
    -- auxiliary store as an ad-hoc sequence of VRefs and binary data. 
    stash :: a -> VPut ()

    -- | Cache loads a value from a stashed representation in an auxiliary 
    -- address space back to the normal value representation in Haskell's
    -- heap. This operation must load bytes and VRefs in the same quantity
    -- and ordering as they were stashed, and must result in an equivalent
    -- value.
    --
    -- Developers should ensure that caching is backwards compatible for
    -- all versions of a type. This might be achieved by recording type
    -- information, or by using a more generic intermediate structure.
    cache :: VGet a

