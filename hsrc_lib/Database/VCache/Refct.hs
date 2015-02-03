
-- Code for reading and writing reference counts.
module Database.VCache.Refct 
    ( readRefctBytes
    , toRefctBytes
    , writeRefctBytes
    , Refct
    ) where

import Control.Exception
import Data.Word
import Data.Bits
import Foreign.Ptr
import Foreign.Storable
import Database.LMDB.Raw

type Refct = Int

-- simple variable-width encoding for reference counts. Encoding is 
-- base256 then adding 1. (The lead byte should always be non-zero,
-- but that isn't checked here.)
readRefctBytes :: MDB_val -> IO Refct
readRefctBytes v = rd (mv_data v) (mv_size v) 0 where
    rd _ 0 rc = return (rc + 1)
    rd p n rc = do
        w8 <- peek p
        let rc' = (rc `shiftL` 8) .|. (fromIntegral w8) 
        rd (p `plusPtr` 1) (n - 1) rc'

-- compute list of bytes for a big-endian encoding. 
-- Reference count must be positive!
toRefctBytes :: Refct -> [Word8]
toRefctBytes = rcb [] . subtract 1 . assertPositive where
    rcb l 0 = l
    rcb l rc = rcb (fromIntegral rc : l) (rc `shiftR` 8)
    assertPositive n = assert (n > 0) n

-- write a reference variable-width count into an MDB_val.
--
-- Note: given buffer should be large enough for any reference count. 
writeRefctBytes :: Ptr Word8 -> Refct -> IO MDB_val
writeRefctBytes p0 = wrcb p0 0 . toRefctBytes where
    wrcb _ n [] = return $! MDB_val { mv_data = p0, mv_size = n }
    wrcb p n (b:bs) = do { poke p b ; wrcb (p `plusPtr` 1) (n + 1) bs }

