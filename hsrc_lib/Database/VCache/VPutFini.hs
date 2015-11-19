{-# LANGUAGE BangPatterns #-}

module Database.VCache.VPutFini
    ( vputFini
    , runVPutIO
    , runVPut
    ) where

import Control.Exception (onException)
import Data.IORef
import Data.ByteString (ByteString)
import Foreign.Ptr
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import System.IO.Unsafe (unsafePerformIO)
import qualified Data.ByteString.Internal as BSI

import Database.VCache.Types
import Database.VCache.VPutAux

-- | When we're just about done with VPut, we really have one more
-- task to perform: to output the address list for any contained 
-- PVars and VRefs. These addresses are simply concatenated onto the
-- normal byte output, with a final size value (not including itself)
-- to indicate how far to jump back.
--
-- Actually, we output the first address followed by relative offsets
-- for every following address. This behavior allows us to reduce the
-- serialization costs when addresses are near each other in memory.
--
-- The address list is output in the reverse order of serialization.
-- (This simplifies reading in the same order as serialization without
-- a list reversal operation.)
--
-- It's important that we finalize exactly once for every serialization,
-- and that this be applied before any hash functions.
vputFini :: VPut ()
vputFini = do
    szStart <- peekBufferSize
    lChildren <- peekChildren
    putChildren lChildren
    szFini <- peekBufferSize
    putVarNatR (szFini - szStart)
    -- shrinkBuffer? no need, should GC soon.

-- putChildren receives a stack (last in first out), so we'll just
-- write them in order. Our first address is written as a natural
-- number, then each following address is written as a difference.
-- In most cases, this should reduce the encoding overhead.
putChildren :: [Address] -> VPut ()
putChildren = ini where
    ini [] = return ()
    ini (x:xs) = putVarNat (fromIntegral x) >> go x xs 
    go _ [] = return ()
    go p (x:xs) = 
        let offset = (fromIntegral x) - (fromIntegral p) in
        putVarInt offset >> go x xs

-- Obtain a strict bytestring corresponding to VPut output.
-- Also returns any other computed result, usually `()`.
runVPutIO :: VSpace -> VPut a -> IO (a, ByteString)
runVPutIO vs action = do
    let initialSize = 2000 -- avoid reallocs for small records
    pBuff <- mallocBytes initialSize
    vBuff <- newIORef pBuff
    let s0 = VPutS { vput_space = vs
                   , vput_children = []
                   , vput_buffer = vBuff
                   , vput_target = pBuff
                   , vput_limit = pBuff `plusPtr` initialSize
                   }
    let freeBuff = readIORef vBuff >>= free
    let fullWrite = do { result <- action; vputFini; return result }
    (VPutR r sf) <- _vput fullWrite s0 `onException` freeBuff
    pBuff' <- readIORef vBuff
    let len = vput_target sf `minusPtr` pBuff'
    pBuffR <- reallocBytes pBuff' len -- reclaim unused space
    fpBuff' <- newForeignPtr finalizerFree pBuffR
    let bytes = BSI.fromForeignPtr fpBuff' 0 len
    return (r, bytes)
{-# NOINLINE runVPutIO #-}

runVPut :: VSpace -> VPut a -> (a, ByteString)
runVPut vs action = unsafePerformIO (runVPutIO vs action)

