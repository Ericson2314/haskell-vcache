

module Database.VCache.VPutFini
    ( vputFini
    , runVPutIO
    , runVPut
    ) where

import Control.Exception (onException)
import Data.Bits
import Data.IORef
import Data.ByteString (ByteString)
import Foreign.Ptr
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Database.VCache.Types
import Database.VCache.VPut
import System.IO.Unsafe (unsafePerformIO)
import qualified Data.ByteString.Internal as BSI

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
    szStart <- getBufferSize
    lChildren <- listChildren
    putChildren lChildren
    szFini <- getBufferSize
    putVarNatR (szFini - szStart)
    -- shrinkBuffer

getBufferSize :: VPut Int
getBufferSize = VPut $ \ s ->
    readIORef (vput_buffer s) >>= \ pStart ->
    let size = (vput_target s) `minusPtr` pStart in
    size `seq`
    return (VPutR size s)
{-# INLINE getBufferSize #-}

listChildren :: VPut [PutChild]
listChildren = VPut $ \ s ->
    let r = vput_children s in
    return (VPutR r s)
{-# INLINE listChildren #-}

putChildren :: [PutChild] -> VPut ()
putChildren [] = return ()
putChildren (x:xs) = 
    let addr0 = addressOf x in
    putVarNat (fromIntegral addr0) >>
    putChildren' addr0 xs

-- putChildren after the first, using offsets.
putChildren' :: Address -> [PutChild] -> VPut ()
putChildren' _ [] = return ()
putChildren' prev (x:xs) = 
    let addrX = addressOf x in
    let offset = (fromIntegral addrX) - (fromIntegral prev) in
    putVarInt offset >>
    putChildren' addrX xs

addressOf :: PutChild -> Address
addressOf (PutChild (Left (PVar { pvar_addr = x }))) = x
addressOf (PutChild (Right (VRef { vref_addr = x }))) = x

-- write a varNat, but reversed (little-endian)
putVarNatR :: Int -> VPut ()
putVarNatR n = 
    if (n < 0) then fail "non-negative size expected" else
    let (q,r) = n `divMod` 128 in
    putWord8 (fromIntegral r) >>
    putVarNatR' q

-- put the high bytes for a var nat in reverse
putVarNatR' :: Int -> VPut ()
putVarNatR' n =
    if (n < 1) then return () else
    let (q,r) = n `divMod` 128 in
    putWord8 (0x80 .|. fromIntegral r) >>
    putVarNatR' q


{- 
-- use realloc to shrink the buffer to the minimum size
-- (Is really worth doing? I'll disable it for now.)
shrinkBuffer :: VPut ()
shrinkBuffer = VPut $ \ s ->
    readIORef (vput_buffer s) >>= \ pStart ->
    let size = (vput_target s) `minusPtr` pStart in
    reallocBytes pStart size >>= \ pStart' ->
    writeIORef (vput_buffer s) pStart' >>
    let pLimit = pStart' `plusPtr` size in
    let s' = s { vput_target = pLimit, vput_limit = pLimit } in
    return (VPutR () s')

I should probably evaluate whether this is worthwhile. The buffer will be
free in a very short while, so I think it wouldn't matter so much whether
we shrink it. I'd rather avoid extra interactions with the C allocator,
since it generally requires some synchronization between threads.

-}    

runVPutIO :: VSpace -> VPut a -> IO (a, ByteString, [PutChild])
runVPutIO vs action = do
    let initialSize = 1000 -- avoid reallocs for small data
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
    let runPut = _vput fullWrite s0
    (VPutR r sf) <- runPut `onException` freeBuff
    pBuff' <- readIORef vBuff
    fpBuff' <- newForeignPtr finalizerFree pBuff'
    let len = vput_target sf `minusPtr` pBuff'
    let bytes = BSI.fromForeignPtr fpBuff' 0 len
    return (r, bytes, vput_children sf)
{-# NOINLINE runVPutIO #-}

runVPut :: VSpace -> VPut a -> (a, ByteString, [PutChild])
runVPut vs action = unsafePerformIO (runVPutIO vs action)

