{-# LANGUAGE BangPatterns #-}

-- dependencies of both VPutFini and VPut
module Database.VCache.VPutAux
    ( reserving, reserve
    , unsafePutWord8
    , putWord8
    , putVarNat
    , putVarInt
    , putVarNatR

    , peekBufferSize
    , peekChildren
    ) where

import Control.Applicative
import Data.Bits
import Data.Word
import Data.IORef
import Foreign.Storable
import Foreign.Ptr
import Foreign.Marshal.Alloc

import Database.VCache.Types


reserving :: Int -> VPut a -> VPut a
reserving n op = reserve n >> op
{-# RULES
"reserving >> reserving" forall n1 n2 f g . reserving n1 f >> reserving n2 g = reserving (n1+n2) (f>>g)
 #-}
{-# INLINABLE reserving #-}

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
{-# NOINLINE grow #-}

-- | Store an 8 bit word *assuming* enough space has been reserved.
-- This can be used safely together with 'reserve'.
unsafePutWord8 :: Word8 -> VPut ()
unsafePutWord8 w8 = VPut $ \ s -> 
    let pTgt = vput_target s in
    let s' = s { vput_target = (pTgt `plusPtr` 1) } in
    poke pTgt w8 >>
    return (VPutR () s')
{-# INLINE unsafePutWord8 #-}

-- | Store an 8 bit word.
putWord8 :: Word8 -> VPut ()
putWord8 w8 = reserving 1 $ unsafePutWord8 w8
{-# INLINE putWord8 #-}

-- | Put an arbitrary non-negative integer in 'varint' format associated
-- with Google protocol buffers. This takes one byte for values 0..127,
-- two bytes for 128..16k, etc.. Will fail if given a negative argument.
putVarNat :: Integer -> VPut ()
putVarNat n | (n < 0) = fail $ "putVarNat with " ++ show n
            | otherwise = _putVarNat q >> putWord8 bLo 
  where q   = n `shiftR` 7
        bLo = 0x7f .&. fromIntegral n

_putVarNat :: Integer -> VPut ()
_putVarNat 0 = return ()
_putVarNat n = _putVarNat q >> putWord8 b where
    q = n `shiftR` 7
    b = 0x80 .|. (0x7f .&. fromIntegral n)

-- | Put an arbitrary integer in a 'varint' format associated with
-- Google protocol buffers with zigzag encoding of negative numbers.
-- This takes one byte for values -64..63, two bytes for -8k..8k, 
-- three bytes for -1M..1M, etc.. Very useful if most numbers are
-- near 0.
putVarInt :: Integer -> VPut ()
putVarInt = putVarNat . zigZag
{-# INLINE putVarInt #-}

zigZag :: Integer -> Integer
zigZag n | (n < 0)   = (negate n * 2) - 1
         | otherwise = (n * 2)
{-# INLINE zigZag #-}


-- | write a varNat, but reversed (i.e. little-endian)
--
-- This is only used by VPutFini: the last entry is the size (in bytes)
-- of the children list. But we write backwards so we can later read it
-- from the end of the buffer.
putVarNatR :: Int -> VPut ()
putVarNatR n | (n < 0) = fail $ "putVarNatR with " ++ show n 
             | otherwise = putWord8 bLo >> _putVarNatR q 
  where bLo  = 0x7f .&. fromIntegral n
        q    = n `shiftR` 7

_putVarNatR :: Int -> VPut () 
_putVarNatR 0 = return ()
_putVarNatR n = putWord8 b >> _putVarNatR q where
    b = 0x80 .|. (0x7f .&. fromIntegral n)
    q = n `shiftR` 7

-- | Obtain the number of bytes output by this VPut effort so far.
-- This might be useful if you're breaking data structures up by their
-- serialization sizes. This does not include VRefs or PVars, only
-- raw binary data. See also peekChildCount.
peekBufferSize :: VPut Int
peekBufferSize = VPut $ \ s ->
    readIORef (vput_buffer s) >>= \ pStart ->
    let size = (vput_target s) `minusPtr` pStart in
    size `seq`
    return (VPutR size s)
{-# INLINE peekBufferSize #-}

peekChildren :: VPut [PutChild]
peekChildren = VPut $ \ s ->
    let r = vput_children s in
    return (VPutR r s)
{-# INLINE peekChildren #-}

