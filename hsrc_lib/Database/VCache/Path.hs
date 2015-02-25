
module Database.VCache.Path
    ( vcacheSubdir
    , vcacheSubdirM
    ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Database.VCache.Types

maxPathLen :: Int
maxPathLen = 511  -- key size limit from LMDB 0.9.10

-- | VCache implements a simplified filesystem metaphor. By assigning
-- a different prefix for root PVars loaded by different subprograms,
-- developers can guard against namespace collisions. Each component
-- may have its own persistent roots.
-- 
-- While I call it a subdirectory, it really is just a prefix. Using
-- "foo" followed by "bar" is equivalent to using "foobar". Developers
-- should include their own separators if they expect them, i.e. "foo\/"
-- and "bar\/".
--
-- Paths are limited to ~500 bytes. For normal use, this limit will not
-- be a problem. If you're creating PVars based on runtime inputs, those
-- should always be dynamic PVars. Root PVar names should never be much 
-- larger than fully qualified function names.
--
vcacheSubdir :: ByteString -> VCache -> VCache
vcacheSubdir p (VCache vs d) = 
    let d' = subdir d p in
    if (BS.length d' > maxPathLen) 
        then error ("VCache path too long: " ++ show d')
        else (VCache vs d')

-- | as vcacheSubdir, but returns Nothing if the path is too large.
vcacheSubdirM :: ByteString -> VCache -> Maybe VCache
vcacheSubdirM p (VCache vs d) =
    let d' = subdir d p in
    if (BS.length d' > maxPathLen)
        then Nothing
        else Just (VCache vs d')

subdir :: ByteString -> ByteString -> ByteString
subdir = BS.append
{-# INLINE subdir #-}
