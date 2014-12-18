{-# LANGUAGE ImpredicativeTypes #-}
module Database.VCache.VCacheable
    ( VCacheable(..), VPut, VGet
    ) where

import Control.Applicative
import Control.Monad

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
data VPutR r = VPutR
    { vput_result :: r
    , vput_state  :: !VPutS
    }

-- | VGet represents an action that parses a value from a string of bytes
-- and values. Parser combinators are supported, and are of recursive
-- descent in style. It is possible to isolate a 'get' operation to a subset
-- of values and bytes, requiring a parser to consume exactly the requested
-- quantity of content.
--
-- VGet may fail with a very simple error string. Developers may wrap these
-- failure messages with better contextual information, e.g. about what is
-- expected.
newtype VGet a = VGet { _vget :: VGetS -> IO (VGetR a) }
data VGetS = VGetS 
    { vget_children :: ![Address]
    , vget_target   :: {-# UNPACK #-} !PtrLoc
    , vget_limit    :: {-# UNPACK #-} !PtrEnd
    }
data VGetR r 
    = VGetR r !VGetS
    | VGetE String

instance Functor VPut where 
    fmap f m = VPut $ \ s -> 
        _vput m s >>= \ (VPutR r s') ->
        return (VPutR (f r) s')
    {-# INLINE fmap #-}
instance Applicative VPut where
    pure = return
    (<*>) = ap
    {-# INLINE pure #-}
    {-# INLINE (<*>) #-}
instance Monad VPut where
    return r = VPut (\ s -> return (VPutR r s))
    m >>= k = VPut $ \ s ->
        _vput m s >>= \ (VPutR r s') ->
        _vput (k r) s'
    m >> k = VPut $ \ s ->
        _vput m s >>= \ (VPutR _ s') ->
        _vput k s'
    {-# INLINE return #-}
    {-# INLINE (>>=) #-}
    {-# INLINE (>>) #-}

instance Functor VGet where
    fmap f m = VGet $ \ s ->
        _vget m s >>= \ c ->
        return $ case c of
            VGetR r s' -> VGetR (f r) s'
            VGetE msg -> VGetE msg 
    {-# INLINE fmap #-}
instance Applicative VGet where
    pure = return
    (<*>) = ap
    {-# INLINE pure #-}
    {-# INLINE (<*>) #-}
instance Monad VGet where
    fail msg = VGet (\ _ -> return (VGetE msg))
    return r = VGet (\ s -> return (VGetR r s))
    m >>= k = VGet $ \ s ->
        _vget m s >>= \ c ->
        case c of
            VGetE msg -> return (VGetE msg)
            VGetR r s' -> _vget (k r) s'
    m >> k = VGet $ \ s ->
        _vget m s >>= \ c ->
        case c of
            VGetE msg -> return (VGetE msg)
            VGetR _ s' -> _vget k s'
    {-# INLINE fail #-}
    {-# INLINE return #-}
    {-# INLINE (>>=) #-}
    {-# INLINE (>>) #-}
instance Alternative VGet where
    empty = mzero
    (<|>) = mplus
    {-# INLINE empty #-}
    {-# INLINE (<|>) #-}
instance MonadPlus VGet where
    mzero = fail "mzero"
    mplus f g = VGet $ \ s ->
        _vget f s >>= \ c ->
        case c of
            VGetE _ -> _vget g s
            r -> return r
    {-# INLINE mzero #-}
    {-# INLINE mplus #-}
    

    


