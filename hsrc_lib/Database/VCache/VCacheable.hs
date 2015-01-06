{-# LANGUAGE BangPatterns #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Database.VCache.VCacheable
    ( VCacheable(..), VPut, VGet

    -- * Prim Writers
    , putVRef, putVRef'
    , putWord8
    , putWord16le, putWord16be
    , putWord32le, putWord32be
    , putWord64le, putWord64be
    , putStorable
    , putVarNat, putVarInt
    , reserve, unsafePutWord8
    , putByteString, putByteStringLazy
    
    -- * Prim Readers
    , getVRef
    , getWord8
    , getWord16le, getWord16be
    , getWord32le, getWord32be
    , getWord64le, getWord64be
    , getStorable
    , getVarNat, getVarInt
    , getByteString, getByteStringLazy
    , isEmpty

    -- * Parser Combinators
    , isolate
    , label
    , lookAhead, lookAheadM, lookAheadE
    ) where

import Control.Applicative
import Control.Monad

import Data.Bits
import Data.Char
import Data.Word
import Data.Typeable
import Data.IORef
import Database.VCache.Types
import Foreign.Ptr (plusPtr,minusPtr,castPtr,Ptr)
import Foreign.Storable (Storable(..))
import Foreign.Marshal.Alloc (alloca,mallocBytes,reallocBytes,finalizerFree)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.ForeignPtr (newForeignPtr,withForeignPtr)

import qualified Data.List as L
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Lazy as LBS

import Database.VCache.Types
import Database.VCache.Impl


-- VPut and VGet defined in VCache.Types

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

-- | Store a reference to a value. The value will be copied implicitly
-- to the target address space, if necessary.
putVRef :: VRef a -> VPut ()
putVRef r = VPut $ \ s ->
    let r' = mvref (vput_space s) r in
    _putVRef s (VRef_ r')
{-# INLINE putVRef #-}

-- | Store a reference to a value that you know is already part of the
-- destination address space. This operation fails if the input is not
-- already part of the destination space. This is useful for reasoning
-- about performance, but shifts some burden to the developer to ensure
-- all values have one location.
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

-- TODO: VCacheable for Char, ByteString, etc.

-- | isolate a parser to a subset of bytes and value references. The
-- child parser must process its entire input (all bytes and values) 
-- or will fail. If there is not enough available input to isolate, 
-- this operation will fail. 
--
--      isolate nBytes nVRefs operation
--
isolate :: Int -> Int -> VGet a -> VGet a
isolate nBytes nRefs op = VGet $ \ s ->
    let pF = vget_target s `plusPtr` nBytes in
    if (pF > vget_limit s) then return (VGetE "isolate: not enough data") else
    case takeExact nRefs (vget_children s) of
        Nothing -> return (VGetE "isolate: not enough children")
        Just (cs,cs') -> 
            let s_isolated = s { vget_children = cs
                               , vget_limit  = pF
                               }
            in
            let s_postIsolate = s { vget_children = cs'
                                  , vget_target = pF
                                  }
            in
            _vget op s_isolated >>= \ r_isolated -> return $ 
            case r_isolated of
                VGetE emsg -> VGetE emsg
                VGetR r s' ->
                    let bFullParse = (vget_target s' == pF)
                                  && (L.null (vget_children s'))
                    in
                    if bFullParse then VGetR r s_postIsolate
                                  else VGetE "isolate: did not parse all input"

-- take exactly the requested amount from a list, or return Nothing.
takeExact :: Int -> [a] -> Maybe ([a],[a])
takeExact = takeExact' [] 
{-# INLINE takeExact #-}

takeExact' :: [a] -> Int -> [a] -> Maybe ([a],[a])
takeExact' l 0 r = Just (L.reverse l, r)
takeExact' l n (r:rs) = takeExact' (r:l) (n-1) rs
takeExact' _ _ _ = Nothing

-- consuming a number of bytes (for unsafe VGet operations)
--  does not perform a full isolation
consuming :: Int -> VGet a -> VGet a
consuming n op = VGet $ \ s ->
    let pConsuming = vget_target s `plusPtr` n in
    if (pConsuming > vget_limit s) then return (VGetE "not enough data") else 
    _vget op s 
{-# RULES
"consuming.consuming"   forall n1 n2 op . consuming n1 (consuming n2 op) = consuming (max n1 n2) op
"consuming>>consuming"  forall n1 n2 f g . consuming n1 f >> consuming n2 g = consuming (n1+n2) (f>>g)
"consuming>>=consuming" forall n1 n2 f g . consuming n1 f >>= consuming n2 . g = consuming (n1+n2) (f>>=g)
 #-}
{-# INLINE consuming #-}

-- | Read one byte of data, or fail if not enough data.
getWord8 :: VGet Word8 
getWord8 = consuming 1 $ VGet $ \ s -> do
    let p = vget_target s
    r <- peekByte p
    let s' = s { vget_target = p `plusPtr` 1 }
    return (VGetR r s')
{-# INLINE getWord8 #-}

-- | Read a VRef. VCache assumes you know the type for referenced
-- child values, e.g. due to bytes written prior. Fails if there 
-- aren't enough child references in the input.
getVRef :: (VCacheable a) => VGet (VRef a)
getVRef = VGet $ \ s -> 
    case (vget_children s) of
        [] -> return (VGetE "not enough children")
        (c:cs) -> do
            let s' = s { vget_children = cs }
            r <- addr2vref (vget_space s) c
            return (VGetR r s')
{-# INLINE getVRef #-}

-- | Read words of size 16, 32, or 64 in little-endian or big-endian.
getWord16le, getWord16be :: VGet Word16
getWord32le, getWord32be :: VGet Word32
getWord64le, getWord64be :: VGet Word64

getWord16le = consuming 2 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    let r = (fromIntegral b1 `shiftL`  8) .|.
            (fromIntegral b0            )
    let s' = s { vget_target = p `plusPtr` 2 }
    return (VGetR r s')
{-# INLINE getWord16le #-}

getWord32le = consuming 4 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    b2 <- peekByte (p `plusPtr` 2)
    b3 <- peekByte (p `plusPtr` 3)
    let r = (fromIntegral b3 `shiftL` 24) .|.
            (fromIntegral b2 `shiftL` 16) .|.
            (fromIntegral b1 `shiftL`  8) .|.
            (fromIntegral b0            )
    let s' = s { vget_target = p `plusPtr` 4 }
    return (VGetR r s')
{-# INLINE getWord32le #-}

getWord64le = consuming 8 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    b2 <- peekByte (p `plusPtr` 2)
    b3 <- peekByte (p `plusPtr` 3)
    b4 <- peekByte (p `plusPtr` 4)
    b5 <- peekByte (p `plusPtr` 5)
    b6 <- peekByte (p `plusPtr` 6)
    b7 <- peekByte (p `plusPtr` 7)
    let r = (fromIntegral b7 `shiftL` 56) .|.
            (fromIntegral b6 `shiftL` 48) .|.
            (fromIntegral b5 `shiftL` 40) .|.
            (fromIntegral b4 `shiftL` 32) .|.
            (fromIntegral b3 `shiftL` 24) .|.
            (fromIntegral b2 `shiftL` 16) .|.
            (fromIntegral b1 `shiftL`  8) .|.
            (fromIntegral b0            )    
    let s' = s { vget_target = p `plusPtr` 8 }
    return (VGetR r s')
{-# INLINE getWord64le #-}

getWord16be = consuming 2 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    let r = (fromIntegral b0 `shiftL`  8) .|.
            (fromIntegral b1            )
    let s' = s { vget_target = p `plusPtr` 2 }
    return (VGetR r s')
{-# INLINE getWord16be #-}

getWord32be = consuming 4 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    b2 <- peekByte (p `plusPtr` 2)
    b3 <- peekByte (p `plusPtr` 3)
    let r = (fromIntegral b0 `shiftL` 24) .|.
            (fromIntegral b1 `shiftL` 16) .|.
            (fromIntegral b2 `shiftL`  8) .|.
            (fromIntegral b3            )
    let s' = s { vget_target = p `plusPtr` 4 }
    return (VGetR r s')
{-# INLINE getWord32be #-}

getWord64be = consuming 8 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    b2 <- peekByte (p `plusPtr` 2)
    b3 <- peekByte (p `plusPtr` 3)
    b4 <- peekByte (p `plusPtr` 4)
    b5 <- peekByte (p `plusPtr` 5)
    b6 <- peekByte (p `plusPtr` 6)
    b7 <- peekByte (p `plusPtr` 7)
    let r = (fromIntegral b0 `shiftL` 56) .|.
            (fromIntegral b1 `shiftL` 48) .|.
            (fromIntegral b2 `shiftL` 40) .|.
            (fromIntegral b3 `shiftL` 32) .|.
            (fromIntegral b4 `shiftL` 24) .|.
            (fromIntegral b5 `shiftL` 16) .|.
            (fromIntegral b6 `shiftL`  8) .|.
            (fromIntegral b7            )    
    let s' = s { vget_target = p `plusPtr` 8 }
    return (VGetR r s')
{-# INLINE getWord64be #-}

-- to simplify type inference
peekByte :: Ptr Word8 -> IO Word8
peekByte = peek
{-# INLINE peekByte #-}

-- | Read a Storable value. In this case, the content should be
-- bytes only, since pointers aren't really meaningful when persisted.
-- Data is copied to an intermediate structure via alloca to avoid
-- alignment issues.
getStorable :: (Storable a) => VGet a
getStorable = _getStorable undefined
{-# INLINE getStorable #-}

_getStorable :: (Storable a) => a -> VGet a
_getStorable _dummy = 
    let n = sizeOf _dummy in
    consuming n $ VGet $ \ s ->
        let pTgt = vget_target s in
        alloca $ \ pA -> do
            copyBytes (castPtr pA) pTgt n 
            a <- peek pA
            let s' = s { vget_target = pTgt `plusPtr` n }
            return (VGetR a s')
{-# INLINE _getStorable #-}


-- | Get an integer represented in the Google protocol buffers zigzag
-- 'varint' encoding, e.g. as produced by 'putVarInt'. 
getVarInt :: VGet Integer
getVarInt = unZigZag <$> getVarNat
{-# INLINE getVarInt #-}

-- undo protocol buffers zigzag encoding
unZigZag :: Integer -> Integer
unZigZag n =
    let (q,r) = n `divMod` 2 in
    if (1 == r) then negate q - 1
                else q
{-# INLINE unZigZag #-}

-- | Get a non-negative number represented in the Google protocol
-- buffers 'varint' encoding, e.g. as produced by 'putVarNat'.
getVarNat :: VGet Integer
getVarNat = getVarNat' 0
{-# INLINE getVarNat #-}

-- getVarNat' uses accumulator
getVarNat' :: Integer -> VGet Integer
getVarNat' !n =
    getWord8 >>= \ w ->
    let n' = (128 * n) + fromIntegral (w .&. 0x7f) in
    if (w < 128) then return $! n'
                 else getVarNat' n'

-- | Load a number of bytes from the underlying object. A copy is
-- performed in this case (typically no copy is performed by VGet,
-- but the underlying pointer is ephemeral, becoming invalid after
-- the current read transaction). Fails if not enough data. O(N)
getByteString :: Int -> VGet BS.ByteString
getByteString n | (n > 0)   = _getByteString n
                | otherwise = return (BS.empty)
{-# INLINE getByteString #-}

_getByteString :: Int -> VGet BS.ByteString
_getByteString n = consuming n $ VGet $ \ s -> do
    let pSrc = vget_target s
    pDst <- mallocBytes n
    copyBytes pDst pSrc n
    fp <- newForeignPtr finalizerFree pDst
    let r = BSI.fromForeignPtr fp 0 n
    let s' = s { vget_target = (pSrc `plusPtr` n) }
    return (VGetR r s')

-- | Get a lazy bytestring. (Simple wrapper on strict bytestring.)
getByteStringLazy :: Int -> VGet LBS.ByteString
getByteStringLazy n = LBS.fromStrict <$> getByteString n
{-# INLINE getByteStringLazy #-}




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

-- | Get a character from UTF-8 format. Assumes a valid encoding.
-- (In case of invalid encoding, arbitrary characters may be returned.)
getc :: VGet Char
getc = 
    _c0 >>= \ b0 ->
    if (b0 < 0x80) then return $! chr b0 else
    if (b0 < 0xe0) then _getc2 (b0 `xor` 0xc0) else
    if (b0 < 0xf0) then _getc3 (b0 `xor` 0xe0) else
    _getc4 (b0 `xor` 0xf0)

-- get UTF-8 of size 2,3,4 bytes
_getc2, _getc3, _getc4 :: Int -> VGet Char
_getc2 b0 =
    _cc >>= \ b1 ->
    return $! chr ((b0 `shiftL` 6) .|. b1)
_getc3 b0 =
    _cc >>= \ b1 ->
    _cc >>= \ b2 ->
    return $! chr ((b0 `shiftL` 12) .|. (b1 `shiftL` 6) .|. b2)
_getc4 b0 =
    _cc >>= \ b1 ->
    _cc >>= \ b2 ->
    _cc >>= \ b3 ->
    return $! chr ((b0 `shiftL` 18) .|. (b1 `shiftL` 12) .|. (b2 `shiftL` 6) .|. b3)

_c0,_cc :: VGet Int
_c0 = fromIntegral <$> getWord8
_cc = (fromIntegral . xor 0x80) <$> getWord8
{-# INLINE _c0 #-}
{-# INLINE _cc #-}


-- | label will modify the error message returned from the
-- argument operation; it can help contextualize parse errors.
label :: ShowS -> VGet a -> VGet a
label sf op = VGet $ \ s ->
    _vget op s >>= \ r ->
    return $
    case r of
        VGetE emsg -> VGetE (sf emsg)
        ok@(VGetR _ _) -> ok

-- | lookAhead will parse a value, but not consume any input.
lookAhead :: VGet a -> VGet a
lookAhead op = VGet $ \ s ->
    _vget op s >>= \ result -> 
    return $
    case result of
        VGetR r _ -> VGetR r s
        other -> other

-- | lookAheadM will consume input only if it returns `Just a`.
lookAheadM :: VGet (Maybe a) -> VGet (Maybe a)
lookAheadM op = VGet $ \ s ->
    _vget op s >>= \ result -> 
    return $
    case result of
        VGetR Nothing _ -> VGetR Nothing s
        other -> other

-- | lookAheadE will consume input only if it returns `Right b`.
lookAheadE :: VGet (Either a b) -> VGet (Either a b)
lookAheadE op = VGet $ \ s ->
    _vget op s >>= \ result ->
    return $
    case result of
        VGetR l@(Left _) _ -> VGetR l s
        other -> other

-- | isEmpty will return True iff there is no available input (neither
-- references nor values).
isEmpty :: VGet Bool
isEmpty = VGet $ \ s ->
    let bEOF = (vget_target s == vget_limit s) 
            && (L.null (vget_children s))
    in
    bEOF `seq` return (VGetR bEOF s)

instance VCacheable Int where
    get = fromIntegral <$> getVarInt
    put = putVarInt . fromIntegral

instance VCacheable Integer where
    get = getVarInt
    put = putVarInt

instance VCacheable Char where 
    get = getc
    put = putc

instance VCacheable BS.ByteString where
    get = getVarNat >>= getByteString . fromIntegral
    put s = putVarNat (fromIntegral $ BS.length s) >> putByteString s 

instance VCacheable LBS.ByteString where
    get = getVarNat >>= getByteStringLazy . fromIntegral
    put s = putVarNat (fromIntegral $ LBS.length s) >> putByteStringLazy s

instance (VCacheable a) => VCacheable (VRef a) where
    get = getVRef
    put = putVRef

instance (VCacheable a) => VCacheable (Maybe a) where
    get = getWord8 >>= \ jn ->
          if (jn == fromIntegral (ord 'J')) then Just <$> get else
          if (jn == fromIntegral (ord 'N')) then return Nothing else
          fail "Type `Maybe a` expects prefix J or N"
    put (Just a) = putWord8 (fromIntegral (ord 'J')) >> put a
    put Nothing  = putWord8 (fromIntegral (ord 'N'))

instance (VCacheable a, VCacheable b) => VCacheable (Either a b) where
    get = getWord8 >>= \ lr ->
          if (lr == fromIntegral (ord 'L')) then Left <$> get else
          if (lr == fromIntegral (ord 'R')) then Right <$> get else
          fail "Type `Either a b` expects prefix L or R"
    put (Left a) = putWord8 (fromIntegral (ord 'L')) >> put a
    put (Right b) = putWord8 (fromIntegral (ord 'R')) >> put b

instance (VCacheable a, VCacheable b) => VCacheable (a,b) where
    get = (,) <$> get <*> get
    put (a,b) = put a >> put b

instance (VCacheable a, VCacheable b, VCacheable c) => VCacheable (a,b,c) where
    get = (,,) <$> get <*> get <*> get
    put (a,b,c) = put a >> put b >> put c

instance (VCacheable a, VCacheable b, VCacheable c, VCacheable d) => VCacheable (a,b,c,d) where
    get = (,,,) <$> get <*> get <*> get <*> get
    put (a,b,c,d) = put a >> put b >> put c >> put d
