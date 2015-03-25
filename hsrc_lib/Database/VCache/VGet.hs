{-# LANGUAGE BangPatterns #-}

module Database.VCache.VGet 
    ( VGet

    -- * Prim Readers
    , getVRef, getPVar
    , getVSpace
    , getWord8
    , getWord16le, getWord16be
    , getWord32le, getWord32be
    , getWord64le, getWord64be
    , getStorable
    , getVarNat, getVarInt
    , getByteString, getByteStringLazy
    , getc

    -- * zero copy access
    , withBytes

    -- * Parser Combinators
    , isolate
    , label
    , lookAhead, lookAheadM, lookAheadE
    , isEmpty

    ) where

import Control.Applicative

import Data.Bits
import Data.Char
import Data.Word
import Foreign.Ptr
import Foreign.Storable (Storable(..))
import Foreign.Marshal.Alloc (mallocBytes,finalizerFree)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.ForeignPtr (newForeignPtr)

import qualified Data.List as L
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Lazy as LBS

import Database.VCache.Types
import Database.VCache.Aligned
import Database.VCache.Alloc
import Database.VCache.VGetAux 

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
            _vget op s_isolated >>= \ r_isolated -> case r_isolated of
                VGetE emsg -> return (VGetE emsg)
                VGetR r s' ->
                    let bDone = vgetStateEmpty s' in
                    if bDone then return (VGetR r s_postIsolate) else
                    return (VGetE "isolate: did not parse all input")

-- take exactly the requested amount from a list, or return Nothing.
takeExact :: Int -> [a] -> Maybe ([a],[a])
takeExact = takeExact' [] 
{-# INLINE takeExact #-}

takeExact' :: [a] -> Int -> [a] -> Maybe ([a],[a])
takeExact' l 0 r = Just (L.reverse l, r)
takeExact' l n (r:rs) = takeExact' (r:l) (n-1) rs
takeExact' _ _ _ = Nothing

-- | Load a VRef, just the reference rather than the content. User must
-- know the type of the value, since getVRef is essentially a typecast.
-- VRef content is not read until deref. 
--
-- All instances of a VRef with the same type and address will share the
-- same cache.
getVRef :: (VCacheable a) => VGet (VRef a)
getVRef = VGet $ \ s -> 
    case (vget_children s) of
        (c:cs) | isVRefAddr c -> do
            let s' = s { vget_children = cs }
            vref <- addr2vref (vget_space s) c
            return (VGetR vref s')
        _ -> return (VGetE "getVRef")
{-# INLINABLE getVRef #-}

-- | Load a PVar, just the variable. Content is loaded lazily on first
-- read, then kept in memory until the PVar is GC'd. Unlike other Haskell
-- variables, PVars can be serialized to the VCache address space. All 
-- PVars for a specific address are collapsed, using the same TVar.
--
-- Developers must know the type of the PVar, since getPVar will cast to
-- any cacheable type. A runtime error is raised only if you attempt to
-- load the same PVar address with two different types.
--
getPVar :: (VCacheable a) => VGet (PVar a) 
getPVar = VGet $ \ s ->
    case (vget_children s) of
        (c:cs) | isPVarAddr c -> do
            let s' = s { vget_children = cs }
            pvar <- addr2pvar (vget_space s) c
            return (VGetR pvar s')
        _ -> return (VGetE "getPVar")
{-# INLINABLE getPVar #-}

-- | Obtain the VSpace associated with content being read. Does not
-- consume any data.
getVSpace :: VGet VSpace 
getVSpace = VGet $ \ s -> return (VGetR (vget_space s) s)
{-# INLINE getVSpace #-}

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

-- | Read a Storable value. In this case, the content should be
-- bytes only, since pointers aren't really meaningful when persisted.
-- Data is copied to an intermediate structure via alloca to avoid
-- alignment issues.
getStorable :: (Storable a) => VGet a
getStorable = _getStorable undefined
{-# INLINE getStorable #-}

_getStorable :: (Storable a) => a -> VGet a
_getStorable _dummy = withBytes (sizeOf _dummy) (peekAligned . castPtr)
{-# INLINE _getStorable #-}

-- | Load a number of bytes from the underlying object. A copy is
-- performed in this case (typically no copy is performed by VGet,
-- but the underlying pointer is ephemeral, becoming invalid after
-- the current read transaction). Fails if not enough data. O(N)
getByteString :: Int -> VGet BS.ByteString
getByteString n | (n > 0)   = _getByteString n
                | otherwise = return (BS.empty)
{-# INLINE getByteString #-}

_getByteString :: Int -> VGet BS.ByteString
_getByteString n = withBytes n $ \ pSrc -> do
    pDst <- mallocBytes n
    copyBytes pDst pSrc n
    fp <- newForeignPtr finalizerFree pDst
    return $! BSI.fromForeignPtr fp 0 n

-- | Get a lazy bytestring. (Simple wrapper on strict bytestring.)
getByteStringLazy :: Int -> VGet LBS.ByteString
getByteStringLazy n = LBS.fromStrict <$> getByteString n
{-# INLINE getByteStringLazy #-}

-- | Access a given number of bytes without copying them. These bytes
-- are read-only, and are considered to be consumed upon returning. 
-- The pointer should be considered invalid after returning from the
-- withBytes computation.
withBytes :: Int -> (Ptr Word8 -> IO a) -> VGet a
withBytes n action = consuming n $ VGet $ \ s -> do
    let pTgt = vget_target s  
    let s' = s { vget_target = pTgt `plusPtr` n }
    a <- action pTgt
    return (VGetR a s')

-- | Get a character from UTF-8 format. Assumes a valid encoding.
-- (In case of invalid encoding, arbitrary characters may be returned.)
getc :: VGet Char
getc = 
    _c0 >>= \ b0 ->
    if (b0 < 0x80) then return $! chr b0 else
    if (b0 < 0xe0) then _getc2 (b0 `xor` 0xc0) else
    if (b0 < 0xf0) then _getc3 (b0 `xor` 0xe0) else
    _getc4 (b0 `xor` 0xf0)

-- get UTF-8 of size 2,3,4 bytes
_getc2, _getc3, _getc4 :: Int -> VGet Char
_getc2 b0 =
    _cc >>= \ b1 ->
    return $! chr ((b0 `shiftL` 6) .|. b1)
_getc3 b0 =
    _cc >>= \ b1 ->
    _cc >>= \ b2 ->
    return $! chr ((b0 `shiftL` 12) .|. (b1 `shiftL` 6) .|. b2)
_getc4 b0 =
    _cc >>= \ b1 ->
    _cc >>= \ b2 ->
    _cc >>= \ b3 ->
    return $! chr ((b0 `shiftL` 18) .|. (b1 `shiftL` 12) .|. (b2 `shiftL` 6) .|. b3)

_c0,_cc :: VGet Int
_c0 = fromIntegral <$> getWord8
_cc = (fromIntegral . xor 0x80) <$> getWord8
{-# INLINE _c0 #-}
{-# INLINE _cc #-}



-- | label will modify the error message returned from the
-- argument operation; it can help contextualize parse errors.
label :: ShowS -> VGet a -> VGet a
label sf op = VGet $ \ s ->
    _vget op s >>= \ r ->
    return $
    case r of
        VGetE emsg -> VGetE (sf emsg)
        ok@(VGetR _ _) -> ok

-- | lookAhead will parse a value, but not consume any input.
lookAhead :: VGet a -> VGet a
lookAhead op = VGet $ \ s ->
    _vget op s >>= \ result -> 
    return $
    case result of
        VGetR r _ -> VGetR r s
        other -> other

-- | lookAheadM will consume input only if it returns `Just a`.
lookAheadM :: VGet (Maybe a) -> VGet (Maybe a)
lookAheadM op = VGet $ \ s ->
    _vget op s >>= \ result -> 
    return $
    case result of
        VGetR Nothing _ -> VGetR Nothing s
        other -> other

-- | lookAheadE will consume input only if it returns `Right b`.
lookAheadE :: VGet (Either a b) -> VGet (Either a b)
lookAheadE op = VGet $ \ s ->
    _vget op s >>= \ result ->
    return $
    case result of
        VGetR l@(Left _) _ -> VGetR l s
        other -> other

