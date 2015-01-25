

-- aligned peek/poke variants. These are more expensive, but should
-- be much more portable to different machines. I might later use
-- #ifdef flags to transparently control alignment sensitivity if
-- this becomes a big performance issue.
module Database.VCache.Aligned
    ( peekAligned
    , pokeAligned
    ) where

import Foreign.Storable
import Foreign.Marshal.Alloc
import Foreign.Marshal.Utils
import Foreign.Ptr

-- | An alignment-sensitive peek; will copy data into aligned 
-- memory prior to performing 'peek'.
peekAligned :: (Storable a) => Ptr a -> IO a
peekAligned = peekAligned' undefined
{-# INLINE peekAligned #-}

-- | An alignment-sensitive poke. Will poke data into aligned
-- memory then copy it into the destination memory.
pokeAligned :: (Storable a) => Ptr a -> a -> IO ()
pokeAligned = pokeAligned' undefined
{-# INLINE pokeAligned #-}

peekAligned' :: (Storable a) => a -> Ptr a -> IO a
peekAligned' _dummy pSrc = 
    allocaBytesAligned (sizeOf _dummy) (alignment _dummy) $ \ pBuff -> do
        copyBytes pBuff pSrc (sizeOf _dummy)
        peek pBuff
{-# INLINE peekAligned' #-}

pokeAligned' :: (Storable a) => a -> Ptr a -> a -> IO ()
pokeAligned' _dummy pDst a =
    allocaBytesAligned (sizeOf _dummy) (alignment _dummy) $ \ pBuff -> do
        poke pBuff a
        copyBytes pDst pBuff (sizeOf _dummy)
{-# INLINE pokeAligned' #-}
