
-- I don't need a secure hash for content addressing. Here, I'm using
-- murmur3, usually the first 8 bytes thereof.
module Database.VCache.Hash
    ( hash
    ) where

import qualified Data.Digest.Murmur3 as M3
import qualified Data.ByteString as B

hash :: B.ByteString -> B.ByteString
hash = M3.asByteString . M3.hash
