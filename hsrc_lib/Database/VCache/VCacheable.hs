{-# LANGUAGE BangPatterns #-}

module Database.VCache.VCacheable
    ( VCacheable(..), VPut, VGet

    -- * Basic Writers
    , putVRef, putVRef'
    , putWord8
    , putWord16le, putWord16be
    , putWord32le, putWord32be
    , putWord64le, putWord64be
    , putStorable
    , putVarNat, putVarInt
    , reserve, unsafePutWord8
    
    -- * Basic Readers
    , getVRef
    , getWord8
    , getWord16le, getWord16be
    , getWord32le, getWord32be
    , getWord64le, getWord64be
    , getStorable
    , getVarNat, getVarInt
    , isolate
{-
    , putByteString, getByteString
    , putByteStringLazy, getByteStringLazy
    , getRemainingBytes, getRemainingBytesLazy
    , putChar, getChar
    , putString, getString
-}
    ) where

import Control.Applicative
import Control.Monad

import Data.Bits
import Data.Word
import Data.Typeable
import Data.IORef
import Database.VCache.Types
import Foreign.Ptr
import Foreign.Storable
import Foreign.Marshal.Alloc
import Foreign.Marshal.Utils (copyBytes)

import qualified Data.List as L
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import Database.VCache.Types
import Database.VCache.Impl

-- | To be utilized with VCache, a value must be serializable as a 
-- simple sequence of binary data and child VRefs. Also, to put then
-- get a value must result in equivalent values. Further, values are
-- Typeable to support memory caching of values loaded.
-- 
-- Under the hood, structured data is serialized as the pair:
--
--    (ByteString,[VRef])
--
-- This separation is mostly to simplify garbage collection. However,
-- it also means there is no risk of reading VRef addresses as binary
-- data, nor vice versa. Serialized values may have arbitrary size.
--
-- Developers must ensure that `get` on the output of `put` will return
-- an equivalent value. If a data type isn't stable, developers should
-- consider adding some version information (cf. SafeCopy package) and
-- ensuring backwards compatibility of `get`.
-- 
class (Typeable a) => VCacheable a where 
    -- | Serialize a value as a stream of bytes and value references. 
    put :: a -> VPut ()

    -- | Parse a value from its serialized representation into memory.
    get :: VGet a

-- VPut and VGet defined in VCache.Types


-- | Ensure that at least N bytes are available for storage without
-- growing the underlying buffer. Use this before unsafePutWord8 
-- and similar operations. If you reserve ahead of time to avoid
-- dynamic allocations, be sure to account for 8 bytes per VRef plus
-- a count of VRefs (as a varNat).
reserve :: Int -> VPut ()
reserve n = VPut $ \ s ->
    let avail = vput_limit s `minusPtr` vput_target s in
    if (avail >= n) then return (VPutR () s) 
                    else VPutR () <$> grow n s 
{-# INLINE reserve #-}

grow :: Int -> VPutS -> IO VPutS
grow n s =
    readIORef (vput_buffer s) >>= \ pBuff ->
    let currSize = vput_limit s `minusPtr` pBuff in
    let bytesUsed = vput_target s `minusPtr` pBuff in
    let bytesNeeded = (2 * currSize) + n in
    reallocBytes pBuff bytesNeeded >>= \ pBuff' ->
    -- (realloc will throw if it fails)
    writeIORef (vput_buffer s) pBuff' >>
    let target' = pBuff' `plusPtr` bytesUsed in
    let limit' = pBuff' `plusPtr` bytesNeeded in
    return $ s
        { vput_target = target'
        , vput_limit = limit'
        }

-- | Store an 8 bit word.
putWord8 :: Word8 -> VPut ()
putWord8 w8 = reserving 1 $ unsafePutWord8 w8
{-# INLINE putWord8 #-}

-- | Store an 8 bit word *assuming* enough space has been reserved.
-- This can be used safely together with 'reserve'.
unsafePutWord8 :: Word8 -> VPut ()
unsafePutWord8 w8 = VPut $ \ s -> 
    let pTgt = vput_target s in
    poke pTgt w8 >>
    let s' = s { vput_target = (pTgt `plusPtr` 1) } in
    return (VPutR () s')
{-# INLINE unsafePutWord8 #-}

-- | Store a reference to a value. The value will be copied implicitly
-- to the target address space, if necessary.
putVRef :: VRef a -> VPut ()
putVRef r = VPut $ \ s ->
    let r' = mvref (vput_space s) r in
    _putVRef s (VRef_ r')
{-# INLINE putVRef #-}

-- | Store a reference to a value that you know is already part of the
-- destination address space. This operation fails if the input is not
-- already part of the destination space. This is useful for reasoning
-- about performance, but shifts some burden to the developer to ensure
-- all values have one location.
putVRef' :: VRef a -> VPut ()
putVRef' r = VPut $ \ s ->
    if (vput_space s == vref_space r) then _putVRef s (VRef_ r) else
    fail $ "putVRef' argument is not from destination VCache" 
{-# INLINE putVRef' #-}

-- assuming destination and ref have same address space
_putVRef :: VPutS -> VRef_ -> IO (VPutR ())
_putVRef s r = 
    let vs = vput_children s in
    let s' = s { vput_children = (r:vs) } in
    return (VPutR () s')
{-# INLINE _putVRef #-}

-- | Put a Word in little-endian or big-endian form.
--
-- Note: These are mostly included because they're part of the 
-- Data.Binary and Data.Cereal APIs. They may be useful in some
-- cases, but putVarInt will frequently be preferable.
putWord16le, putWord16be :: Word16 -> VPut ()
putWord32le, putWord32be :: Word32 -> VPut ()
putWord64le, putWord64be :: Word64 -> VPut ()

-- THOUGHTS: I could probably optimize these further by using
-- an intermediate type and some rewriting to combine reserve
-- operations. However, I doubt I'll actually use putWord* all
-- that much... mostly just including to match the Data.Cereal
-- and Data.Binary APIs. I expect to use variable-sized integers
-- and such much more frequently.

reserving :: Int -> VPut a -> VPut a
reserving n op = reserve n >> op
{-# RULES
"reserving >> reserving" forall n1 n2 f g . reserving n1 f >> reserving n2 g = reserving (n1+n2) (f>>g)
 #-}

putWord16le w = reserving 2 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w           ) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR` 8) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 2) }
    return (VPutR () s')
{-# INLINE putWord16le #-}

putWord32le w = reserving 4 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w            ) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w `shiftR` 24) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 4) }
    return (VPutR () s')
{-# INLINE putWord32le #-}

putWord64le w = reserving 8 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w            ) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w `shiftR` 24) :: Word8)
    poke (p `plusPtr` 4) (fromIntegral (w `shiftR` 32) :: Word8)
    poke (p `plusPtr` 5) (fromIntegral (w `shiftR` 40) :: Word8)
    poke (p `plusPtr` 6) (fromIntegral (w `shiftR` 48) :: Word8)
    poke (p `plusPtr` 7) (fromIntegral (w `shiftR` 56) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 8) }
    return (VPutR () s')
{-# INLINE putWord64le #-}

putWord16be w = reserving 2 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w `shiftR` 8) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w           ) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 2) }
    return (VPutR () s')
{-# INLINE putWord16be #-}

putWord32be w = reserving 4 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w `shiftR` 24) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w            ) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 4) }
    return (VPutR () s')
{-# INLINE putWord32be #-}

putWord64be w = reserving 8 $ VPut $ \ s -> do
    let p = vput_target s
    poke (p            ) (fromIntegral (w `shiftR` 56) :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (w `shiftR` 48) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (w `shiftR` 40) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (w `shiftR` 32) :: Word8)
    poke (p `plusPtr` 4) (fromIntegral (w `shiftR` 24) :: Word8)
    poke (p `plusPtr` 5) (fromIntegral (w `shiftR` 16) :: Word8)
    poke (p `plusPtr` 6) (fromIntegral (w `shiftR`  8) :: Word8)
    poke (p `plusPtr` 7) (fromIntegral (w            ) :: Word8)
    let s' = s { vput_target = (p `plusPtr` 8) }
    return (VPutR () s')
{-# INLINE putWord64be #-}

-- | Put a Data.Storable value, using intermediate storage to
-- ensure alignment when serializing argument.
putStorable :: (Storable a) => a -> VPut () 
putStorable a = 
    let n = sizeOf a in
    reserving n $ VPut $ \ s ->
        let pTgt = vput_target s in
        let s' = s { vput_target = (pTgt `plusPtr` n) } in
        alloca $ \ pA -> do
            poke pA a
            copyBytes pTgt (castPtr pA) n
            return (VPutR () s')
{-# INLINE putStorable #-}

-- | Put an arbitrary integer in a 'varint' format associated with
-- Google protocol buffers. This takes one byte for values -64..63,
-- two bytes for -8k..8k, three bytes for -1M..1M, etc. and is more
-- efficient than recording a bytecount followed by the input for up
-- to eight bytes. This is especially useful if you can arrange the
-- inputs to mostly be small integers.
putVarInt :: Integer -> VPut ()
putVarInt n | (n < 0)   = _putVarNat $ abs $ 1 + (n * 2)
            | otherwise = _putVarNat (n * 2)
{-# INLINE putVarInt #-}

-- | Put an arbitrary non-negative integer in 'varint' format associated
-- with Google protocol buffers. This takes one byte for values 0..127,
-- two bytes for 128..16k, etc.. Will fail if given a negative argument.
putVarNat :: Integer -> VPut ()
putVarNat n | (n < 0)   = fail $ "putVarNat with " ++ show n
            | otherwise = _putVarNat n
{-# INLINE putVarNat #-}

_putVarNat :: Integer -> VPut ()
_putVarNat n =
    let lBytes = _varNatBytes n in
    reserving (L.length lBytes) $
    mapM_ unsafePutWord8 lBytes

_varNatBytes :: Integer -> [Word8]
_varNatBytes n = 
    let (q,r) = n `divMod` 128 in
    let loByte = fromIntegral r in
    _varNatBytes' [loByte] q
{-# INLINE _varNatBytes #-}

_varNatBytes' :: [Word8] -> Integer -> [Word8]
_varNatBytes' out 0 = out
_varNatBytes' out n = 
    let (q,r) = n `divMod` 128 in
    let byte = 128 + fromIntegral r in
    _varNatBytes' (byte:out) q 

-- TODO: VCacheable for Char, ByteString, etc.

-- | isolate a parser to a subset of bytes and value references. The
-- child parser must process its entire input (all bytes and values) 
-- or will fail. If there is not enough available input to isolate, 
-- this operation will fail. 
--
--      isolate nBytes nVRefs operation
--
isolate :: Int -> Int -> VGet a -> VGet a
isolate nBytes nRefs op = VGet $ \ s ->
    let pF = vget_target s `plusPtr` nBytes in
    if (pF > vget_limit s) then return (VGetE "isolate: not enough data") else
    case takeExact nRefs (vget_children s) of
        Nothing -> return (VGetE "isolate: not enough children")
        Just (cs,cs') -> 
            let s_isolated = s { vget_children = cs
                               , vget_limit  = pF
                               }
            in
            let s_postIsolate = s { vget_children = cs'
                                  , vget_target = pF
                                  }
            in
            _vget op s_isolated >>= \ r_isolated -> return $ 
            case r_isolated of
                VGetE emsg -> VGetE emsg
                VGetR r s' ->
                    let bFullParse = (vget_target s' == pF)
                                  && (L.null (vget_children s'))
                    in
                    if bFullParse then VGetR r s_postIsolate
                                  else VGetE "isolate: did not parse all input"

-- take exactly the requested amount from a list, or return Nothing.
takeExact :: Int -> [a] -> Maybe ([a],[a])
takeExact = takeExact' [] 
{-# INLINE takeExact #-}

takeExact' :: [a] -> Int -> [a] -> Maybe ([a],[a])
takeExact' l 0 r = Just (L.reverse l, r)
takeExact' l n (r:rs) = takeExact' (r:l) (n-1) rs
takeExact' _ _ _ = Nothing

-- consuming a number of bytes (for unsafe VGet operations)
--  does not perform a full isolation
consuming :: Int -> VGet a -> VGet a
consuming n op = VGet $ \ s ->
    let pConsuming = vget_target s `plusPtr` n in
    if (pConsuming > vget_limit s) then return (VGetE "not enough data") else 
    _vget op s 
{-# RULES
"consuming.consuming"   forall n1 n2 op . consuming n1 (consuming n2 op) = consuming (max n1 n2) op
"consuming>>consuming"  forall n1 n2 f g . consuming n1 f >> consuming n2 g = consuming (n1+n2) (f>>g)
"consuming>>=consuming" forall n1 n2 f g . consuming n1 f >>= consuming n2 . g = consuming (n1+n2) (f>>=g)
 #-}
{-# INLINE consuming #-}

-- | Read one byte of data, or fail if not enough data.
getWord8 :: VGet Word8 
getWord8 = consuming 1 $ VGet $ \ s -> do
    let p = vget_target s
    r <- peekByte p
    let s' = s { vget_target = p `plusPtr` 1 }
    return (VGetR r s')
{-# INLINE getWord8 #-}

-- | Read a VRef. VCache assumes you know the type for referenced
-- child values, e.g. due to bytes written prior. Fails if there 
-- aren't enough child references in the input.
getVRef :: (VCacheable a) => VGet (VRef a)
getVRef = VGet $ \ s -> 
    case (vget_children s) of
        [] -> return (VGetE "not enough children")
        (c:cs) -> do
            let s' = s { vget_children = cs }
            r <- addr2vref (vget_space s) c
            return (VGetR r s')
{-# INLINE getVRef #-}

-- load a VRef from an address. Tightly coupled to VCache implementation.
addr2vref :: (VCacheable a) => VSpace -> Address -> IO (VRef a)
addr2vref = error "todo: addr2vref"

-- | Read words of size 16, 32, or 64 in little-endian or big-endian.
getWord16le, getWord16be :: VGet Word16
getWord32le, getWord32be :: VGet Word32
getWord64le, getWord64be :: VGet Word64

getWord16le = consuming 2 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    let r = (fromIntegral b1 `shiftL`  8) .|.
            (fromIntegral b0            )
    let s' = s { vget_target = p `plusPtr` 2 }
    return (VGetR r s')
{-# INLINE getWord16le #-}

getWord32le = consuming 4 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    b2 <- peekByte (p `plusPtr` 2)
    b3 <- peekByte (p `plusPtr` 3)
    let r = (fromIntegral b3 `shiftL` 24) .|.
            (fromIntegral b2 `shiftL` 16) .|.
            (fromIntegral b1 `shiftL`  8) .|.
            (fromIntegral b0            )
    let s' = s { vget_target = p `plusPtr` 4 }
    return (VGetR r s')
{-# INLINE getWord32le #-}

getWord64le = consuming 8 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    b2 <- peekByte (p `plusPtr` 2)
    b3 <- peekByte (p `plusPtr` 3)
    b4 <- peekByte (p `plusPtr` 4)
    b5 <- peekByte (p `plusPtr` 5)
    b6 <- peekByte (p `plusPtr` 6)
    b7 <- peekByte (p `plusPtr` 7)
    let r = (fromIntegral b7 `shiftL` 56) .|.
            (fromIntegral b6 `shiftL` 48) .|.
            (fromIntegral b5 `shiftL` 40) .|.
            (fromIntegral b4 `shiftL` 32) .|.
            (fromIntegral b3 `shiftL` 24) .|.
            (fromIntegral b2 `shiftL` 16) .|.
            (fromIntegral b1 `shiftL`  8) .|.
            (fromIntegral b0            )    
    let s' = s { vget_target = p `plusPtr` 8 }
    return (VGetR r s')
{-# INLINE getWord64le #-}

getWord16be = consuming 2 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    let r = (fromIntegral b0 `shiftL`  8) .|.
            (fromIntegral b1            )
    let s' = s { vget_target = p `plusPtr` 2 }
    return (VGetR r s')
{-# INLINE getWord16be #-}

getWord32be = consuming 4 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    b2 <- peekByte (p `plusPtr` 2)
    b3 <- peekByte (p `plusPtr` 3)
    let r = (fromIntegral b0 `shiftL` 24) .|.
            (fromIntegral b1 `shiftL` 16) .|.
            (fromIntegral b2 `shiftL`  8) .|.
            (fromIntegral b3            )
    let s' = s { vget_target = p `plusPtr` 4 }
    return (VGetR r s')
{-# INLINE getWord32be #-}

getWord64be = consuming 8 $ VGet $ \ s -> do
    let p = vget_target s
    b0 <- peekByte p
    b1 <- peekByte (p `plusPtr` 1)
    b2 <- peekByte (p `plusPtr` 2)
    b3 <- peekByte (p `plusPtr` 3)
    b4 <- peekByte (p `plusPtr` 4)
    b5 <- peekByte (p `plusPtr` 5)
    b6 <- peekByte (p `plusPtr` 6)
    b7 <- peekByte (p `plusPtr` 7)
    let r = (fromIntegral b0 `shiftL` 56) .|.
            (fromIntegral b1 `shiftL` 48) .|.
            (fromIntegral b2 `shiftL` 40) .|.
            (fromIntegral b3 `shiftL` 32) .|.
            (fromIntegral b4 `shiftL` 24) .|.
            (fromIntegral b5 `shiftL` 16) .|.
            (fromIntegral b6 `shiftL`  8) .|.
            (fromIntegral b7            )    
    let s' = s { vget_target = p `plusPtr` 8 }
    return (VGetR r s')
{-# INLINE getWord64be #-}

-- to simplify type inference
peekByte :: Ptr Word8 -> IO Word8
peekByte = peek
{-# INLINE peekByte #-}

-- | Read a Storable value. In this case, the content should be
-- bytes only, since pointers aren't really meaningful when persisted.
-- Data is copied to an intermediate structure via alloca to avoid
-- alignment issues.
getStorable :: (Storable a) => VGet a
getStorable = _getStorable undefined

_getStorable :: (Storable a) => a -> VGet a
_getStorable _dummy = 
    let n = sizeOf _dummy in
    consuming n $ VGet $ \ s ->
        let pTgt = vget_target s in
        alloca $ \ pA -> do
            copyBytes (castPtr pA) pTgt n 
            a <- peek pA
            let s' = s { vget_target = pTgt `plusPtr` n }
            return (VGetR a s')


-- | Get an integer represented in the Google protocol buffers zigzag
-- 'varint' encoding, e.g. as produced by 'putVarInt'. 
getVarInt :: VGet Integer
getVarInt = unZigZag <$> getVarNat
{-# INLINE getVarInt #-}

-- undo protocol buffers zigzag encoding
unZigZag :: Integer -> Integer
unZigZag n =
    let (q,r) = n `divMod` 2 in
    if (1 == r) then negate q - 1
                else q
{-# INLINE unZigZag #-}

-- | Get a non-negative number represented in the Google protocol
-- buffers 'varint' encoding, e.g. as produced by 'putVarNat'.
getVarNat :: VGet Integer
getVarNat = getVarNat' 0

-- getVarNat' uses accumulator
getVarNat' :: Integer -> VGet Integer
getVarNat' !n =
    getWord8 >>= \ w ->
    if (w < 128) then return $! ((128*n) + fromIntegral w) else
    getVarNat' ((128*n) + fromIntegral (w - 128))

{-
    , putByteString, getByteString
    , putByteStringLazy, getByteStringLazy
    , getRemainingBytes, getRemainingBytesLazy
    , putVarInt, getVarInt
    , putChar, getChar
    , putString, getString
    , putStorable, getStorable
    , putStorables, getStorables

    , putWord16le, getWord16le
    , putWord16be, getWord16be
    , putWord32le, getWord32le
    , putWord32be, getWord32be
    , putWord64le, getWord64le
    , putWord64be, getWord64be


--
isolate :: Int -> 

    

    , putByte, getByte
    , putVRef, getVRef
    , putByteCount, getByteCount
    , putByteString, getByteString
    , putVarNat, getVarNat
    , putVarInt, getVarInt
    , putChar, getChar
    , putString, getString
    , putStorable, getStorable
    , putStorables, getStorables

    , putWord16le, getWord16le
    , putWord16be, getWord16be
    , putWord32le, getWord32le
    , putWord32be, getWord32be
    , putWord64le, getWord64le
    , putWord64be, getWord64be




-}



{-

    ( tryCommit
    , optionMaybe
    , many, many1, manyC
    , manyTil, manyTilEnd
    , satisfy, satisfyMsg, char, eof
    ) where

import Control.Applicative ((<$>),(<|>),(<*>))
import Data.Binary
import Data.Binary.Get
import qualified Data.List as L

-- | tryCommit is a two-phase parser. The first phase, which
-- returns the second-phase parser, is allowed to fail, in which
-- case Nothing is returned. Otherwise, the second phase is
-- required to succeed, i.e. we are committed to it.
--
-- This is used to limit backtracking, e.g. for the `many` or 
-- `optional` combinators.
tryCommit :: Get (Get a) -> Get (Maybe a)
tryCommit runPhase1 = do
    maybePhase2 <- (Just <$> runPhase1) <|> return Nothing
    case maybePhase2 of
        Just runPhase2 -> Just <$> runPhase2
        Nothing -> return Nothing

-- | parse a value that satisfies some refinement condition.
satisfy :: (Binary a) => (a -> Bool) -> Get a
satisfy = satisfyMsg "unexpected value"

-- | satisfy with a given error message on fail
satisfyMsg :: (Binary a) => String -> (a -> Bool) -> Get a 
satisfyMsg errMsg test = 
    get >>= \ a ->
    if test a then return a 
              else fail errMsg

-- | match a particular character
char :: Char -> Get Char
char c = satisfyMsg eMsg (== c) where
    eMsg = '\'' : c : "' expected"

optionMaybe :: Get a -> Get (Maybe a)
optionMaybe op = tryCommit (return <$> op)

-- | parse zero or more entries.
many :: Get a -> Get [a]
many = manyC . (return <$>)

-- | parse one or more entries.
many1 :: Get a -> Get [a]
many1 g = (:) <$> g <*> many g

-- | parse many elements with partial commit (e.g. for LL1 parsers).
--
-- Allowing failure in the middle of parsing an element helps control
-- how far the parser can backtrack.
manyC :: Get (Get a) -> Get [a]
manyC gg = loop [] where
    tryA = tryCommit gg
    loop as = 
        tryA >>= \ mba ->
        case mba of
            Nothing -> return (L.reverse as)
            Just a -> loop (a:as)

-- | parse EOF, i.e. fail if not at end of input
eof :: Get ()
eof = 
    isEmpty >>= \ bEmpty ->
    if bEmpty then return () else 
    fail "end of input expected"

{- -- older version for `cereal` (which lacks a sensible isEmpty)
eof = (peek >> fail emsg) <|> return () where
    peek = lookAhead getWord8
    emsg = "end of input expected"
-}

-- | parseManyTil: try to parse many inputs til we reach a terminal
manyTil :: Get a -> Get end -> Get [a]
manyTil a e = fst <$> manyTilEnd a e

-- | parse many inputs until terminal; return the terminal, too
--
-- Note that parseManyTil* doesn't have the same difficulty as 
-- parseMany since we need to lookahead on elements.
manyTilEnd :: Get a -> Get end -> Get ([a],end)
manyTilEnd getElem atEnd = loop [] where
    loop as = (atEnd >>= \ end -> return (L.reverse as, end))
          <|> (getElem >>= \ a -> loop (a:as))

-}


