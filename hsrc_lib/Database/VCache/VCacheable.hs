{-# LANGUAGE ImpredicativeTypes #-}
module Database.VCache.VCacheable
    ( VCacheable(..), VPut, VGet
    ) where

import Data.Word
import Data.Typeable
import Database.VCache.Types
import Foreign.Ptr

-- thoughts: should I bother with unboxed tuples and such 
-- to speed this up? I'm not sure. Let's leave low level
-- optimization for later

-- | To be utilized with VCache, a value must be serializable as a 
-- simple sequence of binary data and VRefs. Stashing then caching 
-- a value must result in an equivalent value. Values are Typeable
-- to support the memory caching features.
-- 
-- Under the hood, structured data is serialized as a pair:
--
--    (ByteString,[VRef])
--
-- Bytes and VRefs must be loaded in the same order and quantity as
-- they were emitted. However, there is no risk of reading VRef 
-- address as binary data, nor of reading binary data as an address.
-- This structure simplifies garbage collection: the GC doesn't need
-- type information to precisely trace a value.
--
-- This interface is based on Data.Binary and Data.Cereal, albeit is
-- optimized for VCache in particular. 
-- 
class (Typeable a) => VCacheable a where 
    -- | Stash is a function that will serialize a value as a stream of
    -- bytes and references. 
    stash :: a -> VPut ()

    -- | Cache loads a value from a stashed representation in an auxiliary 
    -- address space back to the normal value representation in Haskell's
    -- heap. This operation must load bytes and VRefs in the same quantity
    -- and ordering as they were stashed, and must result in an equivalent
    -- value.
    --
    -- Developers must ensure that cached values are backwards compatible
    -- for future versions of a data type. VCache leaves this concern to a
    -- separate layer, cf. SafeCopy.
    cache :: VGet a

type Ptr8 = Ptr Word8
type PtrIni = Ptr8
type PtrEnd = Ptr8
type PtrLoc = Ptr8

-- | VPut represents an action that serializes a value as a string of
-- bytes and references to smaller values. Very large values can be
-- represented as a composition of other, slightly less large values.
newtype VPut a = VPut { _vput :: VPutS -> IO (VPutR a) }
data VPutS = VPutS 
    { vput_children :: ![VRef_]
    , vput_buffer   :: {-# UNPACK #-} !PtrIni
    , vput_target   :: {-# UNPACK #-} !PtrLoc
    , vput_limit    :: {-# UNPACK #-} !PtrEnd
    }
data VPutR a = VPutR
    { vput_state  :: !VPutS
    , vput_result :: a
    }

-- | VGet represents an action that parses a value from a string of bytes
-- and values. Parser combinators are supported, though are of recursive
-- descent in style (unsuitable for long-running streams). 
newtype VGet a = VGet { _vget ::VGetS -> IO (VGetR a) }
data VGetS = VGetS 
    { vget_children :: ![Address]
    , vget_target   :: {-# UNPACK #-} !PtrLoc
    , vget_limit    :: {-# UNPACK #-} !PtrEnd
    }
data VGetR a 
    = VGetR !a !VGetS
    | VGetError !String




