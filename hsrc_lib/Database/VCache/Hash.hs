
-- This is the hash function for content addressing.
module Database.VCache.Hash
    ( hash
    , hashVal
    ) where

import qualified Data.Digest.Murmur3 as M3
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Foreign.ForeignPtr

import Database.LMDB.Raw (MDB_val(..))

hash :: ByteString -> ByteString
hash = BS.take 8 . M3.asByteString . M3.hash

hashVal :: MDB_val -> IO ByteString
hashVal mv = do
    fp <- newForeignPtr_ (mv_data mv) -- no finalizer
    let bs = BSI.PS fp 0 (fromIntegral (mv_size mv))
    return $! hash bs
