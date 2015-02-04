
module Database.VCache.Path
    ( vcacheSubdir
    , vcacheSubdirM
    ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Database.VCache.Types

maxPathLen :: Int
maxPathLen = 511  -- key size limit from LMDB 0.9.10

-- | VCache implements a simplified filesystem metaphor. Developers 
-- can assign a distinct prefix for all PVars created by the VCache,
-- thus modeling namespaces or subdirectories. Assuming the normal 
-- advice of opening the VCache only once in the main module, these
-- prefixes enable transparent, modular decomposition of a VCache 
-- application without risk of name collisions.
--
-- VCache is simplistic about this: a prefix is appended directly.
-- If developers use subdir "foo" followed by "bar", the result is 
-- the same as "foobar". Separators are left to local conventions.
-- Consider "foo/" and "bar/" to model filesystem subdirectories. 
--                 
-- Paths have a limited maximum size of ~500 bytes, including the
-- final PVar name. A runtime error may be generated for oversized
-- paths. In practice, this should not be an issue. 
--
-- Usage Note: Subdirectories allow developers to control risk of
-- namespace collisions between modules or plugins. But they are not
-- intended for domain data! Avoid dynamically creating directories
-- or named PVars based on runtime data. It's better to push most
-- domain logic and schema into the PVar layer, which is subject to
-- rich type safety, GC, potential versioning, and other benefits.
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
