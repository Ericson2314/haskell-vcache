
-- I don't need a secure hash for content addressing. Here, I'm using
-- murmur3, usually the first 8 bytes thereof.
module Database.VCache.Hash
    ( hash
    ) where

--import Data.Word (Word64)
--import Data.Bits (shiftL, (.|.))
import qualified Data.Digest.Murmur3 as M3
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B

hash :: B.ByteString -> B.ByteString
hash = M3.asByteString . M3.hash

{-
hash :: B.ByteString -> Word64
hash = unsafeWord64be . hashBytes

unsafeWord64be :: B.ByteString -> Word64
unsafeWord64be = \ s ->
    (fromIntegral (s `B.unsafeIndex` 0) `shiftL` 56) .|.
    (fromIntegral (s `B.unsafeIndex` 1) `shiftL` 48) .|.
    (fromIntegral (s `B.unsafeIndex` 2) `shiftL` 40) .|.
    (fromIntegral (s `B.unsafeIndex` 3) `shiftL` 32) .|.
    (fromIntegral (s `B.unsafeIndex` 4) `shiftL` 24) .|.
    (fromIntegral (s `B.unsafeIndex` 5) `shiftL` 16) .|.
    (fromIntegral (s `B.unsafeIndex` 6) `shiftL` 8)  .|.
    (fromIntegral (s `B.unsafeIndex` 7))
-}

