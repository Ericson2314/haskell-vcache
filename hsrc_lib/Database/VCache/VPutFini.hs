

module Database.VCache.VPutFini
    ( vputFini
    ) where

import Data.Bits
import Data.IORef
import Foreign.Ptr
import Database.VCache.Types
import Database.VCache.VPut

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
