
module Database.VCache.VCacheable
    ( VCacheable(..), VPut, VGet
    , putWord8, putVRef
    --, putWord8, putVRef, putVRef'
    --, getWord8, getVRef

    --, isolate
    --, reserve 
    --, unsafePutWord8
    --, unsafeGetWord8
{-
    , putVRef, getVRef
    , putByteString, getByteString
    , putByteStringLazy, getByteStringLazy
    , getRemainingBytes, getRemainingBytesLazy
    , putVarInt, getVarInt
    , putChar, getChar
    , putString, getString
    , putStorable, getStorable
    , putStorables, getStorables

    , putWord16le, getWord16le
    , putWord16be, getWord16be
    , putWord32le, getWord32le
    , putWord32be, getWord32be
    , putWord64le, getWord64le
    , putWord64be, getWord64be
-}
    ) where

import Control.Applicative
import Control.Monad

import Data.Word
import Data.Typeable
import Data.IORef
import Database.VCache.Types
import Foreign.Ptr
import Foreign.Storable
import Foreign.Marshal.Alloc

import Database.VCache.Types
import Database.VCache.Impl

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
--    ([VRef],ByteString)
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

-- VPut and VGet defined under VCache.Types


-- | Ensure that at least N bytes are available for storage without
-- growing the underlying buffer. Use this before unsafePutWord8 
-- and similar operations. If you reserve ahead of time to avoid
-- dynamic allocations, be sure to account for 8 bytes per VRef plus
-- a count of VRefs (as a varNat).
reserve :: Int -> VPut ()
reserve n = VPut $ \ s ->
    let avail = vput_limit s `minusPtr` vput_target s in
    if (avail >= n) then return (VPutR () s) 
                    else VPutR () <$> grow n s 
{-# INLINE reserve #-}

grow :: Int -> VPutS -> IO VPutS
grow n s =
    readIORef (vput_buffer s) >>= \ pBuff ->
    let currSize = vput_limit s `minusPtr` pBuff in
    let bytesUsed = vput_target s `minusPtr` pBuff in
    let bytesNeeded = (2 * currSize) + n in
    reallocBytes pBuff bytesNeeded >>= \ pBuff' ->
    -- (realloc will throw if it fails)
    writeIORef (vput_buffer s) pBuff' >>
    let target' = pBuff' `plusPtr` bytesUsed in
    let limit' = pBuff' `plusPtr` bytesNeeded in
    return $ s
        { vput_target = target'
        , vput_limit = limit'
        }

-- | Store an 8 bit word.
putWord8 :: Word8 -> VPut ()
putWord8 w8 = reserve 1 >> unsafePutWord8 w8
{-# INLINE putWord8 #-}

-- | Store an 8 bit word *assuming* enough space has been reserved.
-- This can be used safely together with 'reserve'.
unsafePutWord8 :: Word8 -> VPut ()
unsafePutWord8 w8 = VPut $ \ s -> 
    let pTgt = vput_target s in
    poke pTgt w8 >>
    let s' = s { vput_target = (pTgt `plusPtr` 1) } in
    return (VPutR () s')
{-# INLINE unsafePutWord8 #-}

-- | Store a reference to a value. The value will be moved implicitly
-- to the target address space, if necessary.
putVRef :: VRef a -> VPut ()
putVRef r = VPut $ \ s ->
    let r' = mvref (vput_space s) r in
    _putVRef s (VRef_ r')
{-# INLINE putVRef #-}

-- | Store a reference to a value that you know is already part of the
-- destination address space. This operation fails if the value is not
-- co-located with the destination, thus avoiding deep-copies but 
-- allowing failure. 
putVRef' :: VRef a -> VPut ()
putVRef' r = VPut $ \ s ->
    if (vput_space s == vref_space r) then _putVRef s (VRef_ r) else
    fail $ "putVRef' argument is not from destination VCache" 
{-# INLINE putVRef' #-}

-- assuming destination and ref have same address space
_putVRef :: VPutS -> VRef_ -> IO (VPutR ())
_putVRef s r = 
    let vs = vput_children s in
    let s' = s { vput_children = (r:vs) } in
    return (VPutR () s')
{-# INLINE _putVRef #-}

{-
    , putByte, getByte
    , putVRef, getVRef
    , putByteString, getByteString
    , putByteStringLazy, getByteStringLazy
    , getRemainingBytes, getRemainingBytesLazy
    , putVarInt, getVarInt
    , putChar, getChar
    , putString, getString
    , putStorable, getStorable
    , putStorables, getStorables

    , putWord16le, getWord16le
    , putWord16be, getWord16be
    , putWord32le, getWord32le
    , putWord32be, getWord32be
    , putWord64le, getWord64le
    , putWord64be, getWord64be


-- | isolate a parser to a subset of bytes and values. The parser
-- must process its entire input (all bytes and values) or it will
-- fail. If there are not enough available inputs or values, this 
-- operation will also fail.
--
--      isolate nBytes nVRefs operation
--
isolate :: Int -> 





    

    , putByte, getByte
    , putVRef, getVRef
    , putByteCount, getByteCount
    , putByteString, getByteString
    , putVarNat, getVarNat
    , putVarInt, getVarInt
    , putChar, getChar
    , putString, getString
    , putStorable, getStorable
    , putStorables, getStorables

    , putWord16le, getWord16le
    , putWord16be, getWord16be
    , putWord32le, getWord32le
    , putWord32be, getWord32be
    , putWord64le, getWord64le
    , putWord64be, getWord64be




-}

