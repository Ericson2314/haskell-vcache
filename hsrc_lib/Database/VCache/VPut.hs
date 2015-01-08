


module Database.VCache.VPut
    ( VPut

    -- * Prim Writers
    , putVRef
    , putWord8
    , putWord16le, putWord16be
    , putWord32le, putWord32be
    , putWord64le, putWord64be
    , putStorable
    , putVarNat, putVarInt
    , reserve, unsafePutWord8
    , putByteString, putByteStringLazy
    , putc
    ) where

import Control.Applicative

import Data.Bits
import Data.Char
import Data.Word
import Data.IORef
import Foreign.Ptr (plusPtr,minusPtr,castPtr)
import Foreign.Storable (Storable(..))
import Foreign.Marshal.Alloc (alloca,reallocBytes)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.ForeignPtr (withForeignPtr)

import qualified Data.List as L
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Lazy as LBS

import Database.VCache.Types
import Database.VCache.Impl

-- | Ensure that at least N bytes are available for storage without
-- growing the underlying buffer. Use this before unsafePutWord8 
-- and similar operations. If the buffer must grow, it will grow
-- exponentially to ensure amortized constant allocation costs.
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
    -- heuristic exponential growth
    let bytesNeeded = (2 * currSize) + n + 1000 in 
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
putWord8 w8 = reserving 1 $ unsafePutWord8 w8
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

-- | Store a reference to a value, which must already be part of the
-- destination's address space. Usually, you should open only one 
-- VCache, so this should not be a problem. Values must not be moved
-- between spaces without explicit copy. 
putVRef :: VRef a -> VPut ()
putVRef r = VPut $ \ s ->
    if (vput_space s == vref_space r) then _putVRef s (VRef_ r) else
    fail $ "putVRef' argument is not from destination VCache" 
{-# INLINE putVRef #-}

-- assuming destination and ref have same address space
_putVRef :: VPutS -> VRef_ -> IO (VPutR ())
_putVRef s r = 
    let vs = vput_children s in
    let s' = s { vput_children = (r:vs) } in
    return (VPutR () s')
{-# INLINE _putVRef #-}

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

reserving :: Int -> VPut a -> VPut a
reserving n op = reserve n >> op
{-# RULES
"reserving >> reserving" forall n1 n2 f g . reserving n1 f >> reserving n2 g = reserving (n1+n2) (f>>g)
 #-}

putWord16le w = reserving 2 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w           ) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR` 8) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 2) }
    return (VPutR () s')
{-# INLINE putWord16le #-}

putWord32le w = reserving 4 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w            ) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w `shiftR` 24) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 4) }
    return (VPutR () s')
{-# INLINE putWord32le #-}

putWord64le w = reserving 8 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w            ) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w `shiftR` 24) :: Word8)
    poke (p `plusPtr` 4) (fromIntegral (w `shiftR` 32) :: Word8)
    poke (p `plusPtr` 5) (fromIntegral (w `shiftR` 40) :: Word8)
    poke (p `plusPtr` 6) (fromIntegral (w `shiftR` 48) :: Word8)
    poke (p `plusPtr` 7) (fromIntegral (w `shiftR` 56) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 8) }
    return (VPutR () s')
{-# INLINE putWord64le #-}

putWord16be w = reserving 2 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w `shiftR` 8) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w           ) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 2) }
    return (VPutR () s')
{-# INLINE putWord16be #-}

putWord32be w = reserving 4 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w `shiftR` 24) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w            ) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 4) }
    return (VPutR () s')
{-# INLINE putWord32be #-}

putWord64be w = reserving 8 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w `shiftR` 56) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR` 48) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR` 40) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w `shiftR` 32) :: Word8)
    poke (p `plusPtr` 4) (fromIntegral (w `shiftR` 24) :: Word8)
    poke (p `plusPtr` 5) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 6) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 7) (fromIntegral (w            ) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 8) }
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
    reserving n $ VPut $ \ s ->
        let pTgt = vput_target s in
        let s' = s { vput_target = (pTgt `plusPtr` n) } in
        alloca $ \ pA -> do
            poke pA a
            copyBytes pTgt (castPtr pA) n
            return (VPutR () s')
{-# INLINE putStorable #-}

-- | Put an arbitrary integer in a 'varint' format associated with
-- Google protocol buffers with zigzag encoding of negative numbers.
-- This takes one byte for values -64..63, two bytes for -8k..8k, 
-- three bytes for -1M..1M, etc.. Very useful if most numbers are
-- near 0.
putVarInt :: Integer -> VPut ()
putVarInt = _putVarNat . zigZag
{-# INLINE putVarInt #-}

zigZag :: Integer -> Integer
zigZag n | (n < 0)   = (negate n * 2) - 1
         | otherwise = (n * 2)
{-# INLINE zigZag #-}

-- | Put an arbitrary non-negative integer in 'varint' format associated
-- with Google protocol buffers. This takes one byte for values 0..127,
-- two bytes for 128..16k, etc.. Will fail if given a negative argument.
putVarNat :: Integer -> VPut ()
putVarNat n | (n < 0)   = fail $ "putVarNat with " ++ show n
            | otherwise = _putVarNat n
{-# INLINE putVarNat #-}

_putVarNat :: Integer -> VPut ()
_putVarNat n =
    let lBytes = _varNatBytes n in
    reserving (L.length lBytes) $
    mapM_ unsafePutWord8 lBytes

_varNatBytes :: Integer -> [Word8]
_varNatBytes n = 
    let (q,r) = n `divMod` 128 in
    let loByte = fromIntegral r in
    _varNatBytes' [loByte] q
{-# INLINE _varNatBytes #-}

_varNatBytes' :: [Word8] -> Integer -> [Word8]
_varNatBytes' out 0 = out
_varNatBytes' out n = 
    let (q,r) = n `divMod` 128 in
    let byte = 128 + fromIntegral r in
    _varNatBytes' (byte:out) q 

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

-- | Put a character in UTF-8 format.
putc :: Char -> VPut ()
putc a | c <= 0x7f      = putWord8 (fromIntegral c)
       | c <= 0x7ff     = reserving 2 $ VPut $ \ s -> do
                            let p  = vput_target s
                            let s' = s { vput_target = (p `plusPtr` 2) } 
                            poke p (0xc0 .|. y)
                            poke p (0x80 .|. z)
                            return (VPutR () s')
       | c <= 0xffff    = reserving 3 $ VPut $ \ s -> do
                            let p  = vput_target s
                            let s' = s { vput_target = (p `plusPtr` 3) }
                            poke p (0xe0 .|. x)
                            poke p (0x80 .|. y)
                            poke p (0x80 .|. z)
                            return (VPutR () s')
       | c <= 0x10ffff  = reserving 4 $ VPut $ \ s -> do
                            let p = vput_target s
                            let s' = s { vput_target = (p `plusPtr` 4) }
                            poke p (0xf0 .|. w)
                            poke p (0x80 .|. x)
                            poke p (0x80 .|. y)
                            poke p (0x80 .|. z)
                            return (VPutR () s')
        | otherwise     = fail "not a valid character" -- shouldn't happen
    where 
        c = ord a
        z, y, x, w :: Word8
        z = fromIntegral (c           .&. 0x3f)
        y = fromIntegral (shiftR c 6  .&. 0x3f)
        x = fromIntegral (shiftR c 12 .&. 0x3f)
        w = fromIntegral (shiftR c 18 .&. 0x7)


