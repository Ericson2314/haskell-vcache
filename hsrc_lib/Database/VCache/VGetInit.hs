

module Database.VCache.VGetInit
    ( vgetInit
    ) where

import Data.Bits
import Data.Word
import Foreign.Ptr
import Foreign.Storable
import Database.VCache.Types
import Database.VCache.VGet

-- | For VGet from the database, we start with just a pointer and a
-- size. To process the VGet data, we also need to read addresses 
-- from a dedicated region. This is encoded from the end, as follows:
--
--     (normal data) addressN offset offset offset offset ... bytes
--                                                           
-- Here 'bytes' is basically a varNat encoded backwards for the
-- number of bytes (not counting 'bytes') back to the start of the
-- first address. This address is then encoded as a varNat, and any
-- offset is encoded as a varInt with the idea of reducing overhead
-- for encoding addresses near to each other in memory.
--
-- Addresses are encoded such that the first address to parse is last
-- in the sequence (thereby avoiding a list reverse operation).
--
-- To read addresses, we simply read the number of bytes from the end,
-- step back that far, then read the initial address and offsets until
-- we get back to the end. This must be performed before we apply the
-- normal read operation for the VGet state. It must be applied exactly
-- once for a given input.
--
vgetInit :: VGet ()
vgetInit =
    readAddrBytes >>= \ nAddrBytes ->
    if (0 == nAddrBytes) then return () else
    VGet $ \ s -> 
        let bUnderflow = nAddrBytes > (vget_limit s `minusPtr` vget_target s) in 
        if bUnderflow then return eBadAddressRegion else 
        let pAddrs = vget_limit s `plusPtr` negate nAddrBytes in
        let sAddrs = s { vget_target = pAddrs } in
        _vget readAddrs sAddrs >>= \ mbAddrs ->
        case mbAddrs of
            VGetR addrs _ ->
                let s' = s { vget_children = addrs, vget_limit = pAddrs } in
                return (VGetR () s')
            VGetE eMsg -> return (VGetE eMsg)
{-# INLINABLE vgetInit #-}

eBadAddressRegion :: VGetR a
eBadAddressRegion = VGetE "VGet: failed to read address region"

readAddrBytes :: VGet Int
readAddrBytes = readAddrBytes' 0
{-# INLINE readAddrBytes #-}

readAddrBytes' :: Int -> VGet Int
readAddrBytes' nAccum = 
    getWord8FromEnd >>= \ w8 ->
    let nAccum' = (128 * nAccum) + (fromIntegral (0x7f .&. w8)) in
    if (w8 < 0x80) then return nAccum' else
    readAddrBytes' nAccum'

getWord8FromEnd :: VGet Word8
getWord8FromEnd = VGet $ \ s ->
    let pSrc = vget_limit s `plusPtr` (-1) in
    let s' = s { vget_limit = pSrc } in
    if (pSrc < vget_target s) then return eBadAddressRegion else
    peek pSrc >>= \ w8 ->
    return (VGetR w8 s')
{-# INLINE getWord8FromEnd #-}

-- read a variable list of at least one address
readAddrs :: VGet [Address]
readAddrs = 
    getVarNat >>= \ nFirst ->
    let addr0 = fromIntegral nFirst in
    addr0 `seq` readAddrs' [addr0] nFirst

-- read address offsets until end of input
readAddrs' :: [Address] -> Integer -> VGet [Address]
readAddrs' addrs nLast =
    isEmpty >>= \ bEmpty ->
    if bEmpty then return addrs else
    getVarInt >>= \ nOff ->
    let nCurr = nLast + nOff in
    let addr = fromIntegral nCurr in
    addr `seq` readAddrs' (addr:addrs) nCurr
