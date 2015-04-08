


module Database.VCache.VPut
    ( VPut

    -- * Prim Writers
    , putVRef, putPVar
    , putVSpace

    , putWord8
    , putWord16le, putWord16be
    , putWord32le, putWord32be
    , putWord64le, putWord64be
    , putStorable
    , putVarNat, putVarInt
    , reserve, reserving, unsafePutWord8
    , putByteString, putByteStringLazy
    , putc

    , peekBufferSize
    , peekChildCount
    ) where

import Control.Applicative
import Data.Bits
import Data.Char
import Data.Word
import qualified Data.List as L
import Foreign.Ptr (plusPtr,castPtr)
import Foreign.Storable (Storable(..))
import Foreign.Marshal.Utils (copyBytes)
import Foreign.ForeignPtr (withForeignPtr)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Lazy as LBS

import Database.VCache.Types
import Database.VCache.Aligned
import Database.VCache.VPutAux
-- import Database.VCache.Impl

-- | Store a reference to a value. The value reference must already
-- use the same VCache and addres space as where you're putting it.
putVRef :: VRef a -> VPut ()
putVRef ref = VPut $ \ s ->
    if (vput_space s == vref_space ref) then _putVRef s ref else
    fail $ "putVRef argument is not from destination VCache" 
{-# INLINABLE putVRef #-}

-- assuming destination and ref have same address space
_putVRef :: VPutS -> VRef a -> IO (VPutR ())
_putVRef s ref = 
    let cs = vput_children s in
    let c  = PutChild (Right ref) in
    let s' = s { vput_children = (c:cs) } in
    return (VPutR () s')
{-# INLINE _putVRef #-}

-- | Store an identifier for a persistent variable in the same VCache
-- and address space.
putPVar :: PVar a -> VPut ()
putPVar pvar = VPut $ \ s ->
    if (vput_space s == pvar_space pvar) then _putPVar s pvar else 
    fail $ "putPVar argument is not from destination VCache"
{-# INLINABLE putPVar #-}

-- assuming destination and var have same address space
_putPVar :: VPutS -> PVar a -> IO (VPutR ())
_putPVar s pvar = 
    let cs = vput_children s in
    let c  = PutChild (Left pvar) in
    let s' = s { vput_children = (c:cs) } in
    return (VPutR () s')
{-# INLINE _putPVar #-}

-- | Put VSpace doesn't actually output anything, but will fail if
-- the target space does not match the given one.
putVSpace :: VSpace -> VPut ()
putVSpace vc = VPut $ \ s ->
    if (vput_space s == vc) then return (VPutR () s) else
    fail $ "putVSpace argument is not same as destination VCache"
{-# INLINE putVSpace #-}

-- | Put a Word in little-endian or big-endian form.
--
-- Note: These are mostly included because they're part of the 
-- Data.Binary and Data.Cereal APIs. They may be useful in some
-- cases, but putVarInt will frequently be preferable.
putWord16le, putWord16be :: Word16 -> VPut ()
putWord32le, putWord32be :: Word32 -> VPut ()
putWord64le, putWord64be :: Word64 -> VPut ()

-- THOUGHTS: I could probably optimize these further by using
-- an intermediate type and some rewriting to combine reserve
-- operations. However, I doubt I'll actually use putWord* all
-- that much... mostly just including to match the Data.Cereal
-- and Data.Binary APIs. I expect to use variable-sized integers
-- and such much more frequently.


putWord16le w = reserving 2 $ VPut $ \ s -> do
    let p = vput_target s
    let s' = s { vput_target = (p `plusPtr` 2) }
    poke (p            ) (fromIntegral (w           ) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR` 8) :: Word8)
    return (VPutR () s')
{-# INLINE putWord16le #-}

putWord32le w = reserving 4 $ VPut $ \ s -> do
    let p = vput_target s
    let s' = s { vput_target = (p `plusPtr` 4) }
    poke (p            ) (fromIntegral (w            ) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w `shiftR` 24) :: Word8)
    return (VPutR () s')
{-# INLINE putWord32le #-}

putWord64le w = reserving 8 $ VPut $ \ s -> do
    let p = vput_target s
    let s' = s { vput_target = (p `plusPtr` 8) }
    poke (p            ) (fromIntegral (w            ) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w `shiftR` 24) :: Word8)
    poke (p `plusPtr` 4) (fromIntegral (w `shiftR` 32) :: Word8)
    poke (p `plusPtr` 5) (fromIntegral (w `shiftR` 40) :: Word8)
    poke (p `plusPtr` 6) (fromIntegral (w `shiftR` 48) :: Word8)
    poke (p `plusPtr` 7) (fromIntegral (w `shiftR` 56) :: Word8)
    return (VPutR () s')
{-# INLINE putWord64le #-}

putWord16be w = reserving 2 $ VPut $ \ s -> do
    let p = vput_target s
    let s' = s { vput_target = (p `plusPtr` 2) }
    poke (p            ) (fromIntegral (w `shiftR` 8) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w           ) :: Word8)
    return (VPutR () s')
{-# INLINE putWord16be #-}

putWord32be w = reserving 4 $ VPut $ \ s -> do
    let p = vput_target s
    let s' = s { vput_target = (p `plusPtr` 4) }
    poke (p            ) (fromIntegral (w `shiftR` 24) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w            ) :: Word8)
    return (VPutR () s')
{-# INLINE putWord32be #-}

putWord64be w = reserving 8 $ VPut $ \ s -> do
    let p = vput_target s
    let s' = s { vput_target = (p `plusPtr` 8) }
    poke (p            ) (fromIntegral (w `shiftR` 56) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR` 48) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR` 40) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w `shiftR` 32) :: Word8)
    poke (p `plusPtr` 4) (fromIntegral (w `shiftR` 24) :: Word8)
    poke (p `plusPtr` 5) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 6) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 7) (fromIntegral (w            ) :: Word8)
    return (VPutR () s')
{-# INLINE putWord64be #-}

-- | Put a Data.Storable value, using intermediate storage to
-- ensure alignment when serializing argument. Note that this
-- shouldn't have any pointers, since serialized pointers won't
-- usually be valid when loaded later. Also, the storable type
-- shouldn't have any gaps (unassigned bytes); uninitialized
-- bytes may interfere with structure sharing in VCache.
putStorable :: (Storable a) => a -> VPut () 
putStorable a = 
    let n = sizeOf a in
    reserving n $ VPut $ \ s -> do
        let pTgt = vput_target s 
        let s' = s { vput_target = (pTgt `plusPtr` n) } 
        pokeAligned (castPtr pTgt) a
        return (VPutR () s')
{-# INLINABLE putStorable #-}

-- | Put the contents of a bytestring directly. Unlike the 'put' method for
-- bytestrings, this does not include size information; just raw bytes.
putByteString :: BS.ByteString -> VPut ()
putByteString s = reserving (BS.length s) (_putByteString s)
{-# INLINE putByteString #-}

-- | Put contents of a lazy bytestring directly. Unlike the 'put' method for
-- bytestrings, this does not include size information; just raw bytes.
putByteStringLazy :: LBS.ByteString -> VPut ()
putByteStringLazy s = reserving (fromIntegral $ LBS.length s) (mapM_ _putByteString (LBS.toChunks s))
{-# INLINE putByteStringLazy #-}

-- put a byte string, assuming enough space has been reserved already.
-- this uses a simple memcpy to the target space.
_putByteString :: BS.ByteString -> VPut ()
_putByteString (BSI.PS fpSrc p_off p_len) = 
    VPut $ \ s -> withForeignPtr fpSrc $ \ pSrc -> do
        let pDst = vput_target s
        copyBytes pDst (pSrc `plusPtr` p_off) p_len
        let s' = s { vput_target = (pDst `plusPtr` p_len) }
        return (VPutR () s')
{-# INLINABLE _putByteString #-}

-- | Put a character in UTF-8 format.
putc :: Char -> VPut ()
putc a | c <= 0x7f      = putWord8 (fromIntegral c)
       | c <= 0x7ff     = reserving 2 $ VPut $ \ s -> do
                            let p  = vput_target s
                            let s' = s { vput_target = (p `plusPtr` 2) } 
                            poke (p            ) (0xc0 .|. y)
                            poke (p `plusPtr` 1) (0x80 .|. z)
                            return (VPutR () s')
       | c <= 0xffff    = reserving 3 $ VPut $ \ s -> do
                            let p  = vput_target s
                            let s' = s { vput_target = (p `plusPtr` 3) }
                            poke (p            ) (0xe0 .|. x)
                            poke (p `plusPtr` 1) (0x80 .|. y)
                            poke (p `plusPtr` 2) (0x80 .|. z)
                            return (VPutR () s')
       | c <= 0x10ffff  = reserving 4 $ VPut $ \ s -> do
                            let p = vput_target s
                            let s' = s { vput_target = (p `plusPtr` 4) }
                            poke (p            ) (0xf0 .|. w)
                            poke (p `plusPtr` 1) (0x80 .|. x)
                            poke (p `plusPtr` 2) (0x80 .|. y)
                            poke (p `plusPtr` 3) (0x80 .|. z)
                            return (VPutR () s')
        | otherwise     = fail "not a valid character" -- shouldn't happen
    where 
        c = ord a
        z, y, x, w :: Word8
        z = fromIntegral (c           .&. 0x3f)
        y = fromIntegral (shiftR c 6  .&. 0x3f)
        x = fromIntegral (shiftR c 12 .&. 0x3f)
        w = fromIntegral (shiftR c 18 .&. 0x7)


-- | Obtain the total count of VRefs and PVars in the VPut buffer.
peekChildCount :: VPut Int
peekChildCount = L.length <$> peekChildren
{-# INLINE peekChildCount #-}

