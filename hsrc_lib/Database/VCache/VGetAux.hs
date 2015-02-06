{-# LANGUAGE BangPatterns #-}
-- This module mostly exists to avoid cyclic dependencies
module Database.VCache.VGetAux 
    ( getWord8FromEnd
    , getWord8
    , isEmpty, vgetStateEmpty
    , getVarNat
    , getVarInt
    , consuming
    , peekByte
    ) where

import Control.Applicative
import Data.Word
import Data.Bits
import qualified Data.List as L
import Foreign.Ptr
import Foreign.Storable
import Database.VCache.Types

-- | Read one byte of data, or fail if not enough data.
getWord8 :: VGet Word8 
getWord8 = consuming 1 $ VGet $ \ s -> do
    let p = vget_target s
    r <- peekByte p
    let s' = s { vget_target = p `plusPtr` 1 }
    return (VGetR r s')
{-# INLINE getWord8 #-}

getWord8FromEnd :: VGet Word8
getWord8FromEnd = consuming 1 $ VGet $ \ s -> do
    let p = vget_limit s `plusPtr` (-1)
    r <- peekByte p
    let s' = s { vget_limit = p }
    return (VGetR r s')
{-# INLINE getWord8FromEnd #-}

-- to simplify type inference
peekByte :: Ptr Word8 -> IO Word8
peekByte = peek
{-# INLINE peekByte #-}

-- | isEmpty will return True iff there is no available input (neither
-- references nor values).
isEmpty :: VGet Bool
isEmpty = VGet $ \ s ->
    let bEOF = vgetStateEmpty s in
    bEOF `seq` return (VGetR bEOF s)
{-# INLINE isEmpty #-}

vgetStateEmpty :: VGetS -> Bool
vgetStateEmpty s = (vget_target s == vget_limit s)
                && (L.null (vget_children s))
{-# INLINE vgetStateEmpty #-}

-- | Get an integer represented in the Google protocol buffers zigzag
-- 'varint' encoding, e.g. as produced by 'putVarInt'. 
getVarInt :: VGet Integer
getVarInt = unZigZag <$> getVarNat
{-# INLINE getVarInt #-}

-- undo protocol buffers zigzag encoding
unZigZag :: Integer -> Integer
unZigZag !n =
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
{-# INLINABLE consuming #-}

