{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE BangPatterns #-}

module Database.VCache.VCacheable
    ( VCacheable(..)
    , module Database.VCache.VGet
    , module Database.VCache.VPut
    ) where

import Control.Applicative
import Control.Monad

import Data.Word
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS

import Database.VCache.VGet
import Database.VCache.VPut
import Database.VCache.Types

-- VCacheable defined in Database.VCache.Types

instance VCacheable Int where
    get = fromIntegral <$> getVarInt
    put = putVarInt . fromIntegral
    {-# INLINE get #-}
    {-# INLINE put #-}

instance VCacheable Integer where
    get = getVarInt
    put = putVarInt
    {-# INLINE get #-}
    {-# INLINE put #-}

instance VCacheable Bool where
    get = getWord8 >>= \ n -> case n of
        0 -> return False
        1 -> return True
        _ -> fail "Boolean expects a 0 or 1 byte"
    put False = putWord8 0
    put True  = putWord8 1

instance VCacheable Char where 
    get = getc
    put = putc
    {-# INLINE get #-}
    {-# INLINE put #-}

instance VCacheable Word8 where
    get = getWord8
    put = putWord8
    {-# INLINE get #-}
    {-# INLINE put #-}

instance VCacheable BS.ByteString where
    get = getVarNat >>= getByteString . fromIntegral
    put s = putVarNat (fromIntegral $ BS.length s) >> putByteString s 
    {-# INLINE get #-}
    {-# INLINE put #-}

instance VCacheable LBS.ByteString where
    get = getVarNat >>= getByteStringLazy . fromIntegral
    put s = putVarNat (fromIntegral $ LBS.length s) >> putByteStringLazy s
    {-# INLINE get #-}
    {-# INLINE put #-}

instance (VCacheable a) => VCacheable (VRef a) where
    get = getVRef
    put = putVRef
    {-# INLINE get #-}
    {-# INLINE put #-}

instance (VCacheable a) => VCacheable (PVar a) where
    get = getPVar
    put = putPVar
    {-# INLINE get #-}
    {-# INLINE put #-}

instance VCacheable VSpace where
    get = getVSpace
    put = putVSpace
    {-# INLINE get #-}
    {-# INLINE put #-}

-- unit is not actually serialized.
instance VCacheable () where
    get = return ()
    put () = return ()
    {-# INLINE get #-}
    {-# INLINE put #-}

-- `Maybe a` may be upgraded transparently to [a], and may share
-- structure with single element lists.
instance (VCacheable a) => VCacheable (Maybe a) where
    get = getWord8 >>= \ n -> case n of 
        0 -> return Nothing
        1 -> Just <$> get
        _ -> fail "Type `Maybe a` expects prefix byte 0 or 1"
    put Nothing  = putWord8 0
    put (Just a) = putWord8 1 >> put a

instance (VCacheable a, VCacheable b) => VCacheable (Either a b) where
    get = getWord8 >>= \ lr -> case lr of
        0 -> Left <$> get
        1 -> Right <$> get
        _ -> fail "Type `Either a b` expects prefix byte 0 or 1"
    put (Left a) = putWord8 0 >> put a
    put (Right b) = putWord8 1 >> put b

-- NOTE: lists are stored in *reverse* order, such that when read
-- the nodes can be directly constructed into normal order without
-- reversing the list, i.e. thus optimizing for read.
instance (VCacheable a) => VCacheable [a] where
    get = do
        nCount <- liftM fromIntegral getVarNat
        replicateReversed [] nCount get
    put ls = do
        let (nCount, lsr) = countAndReverse ls
        putVarNat (fromIntegral nCount)
        mapM_ put lsr
    {-# INLINE get #-}
    {-# INLINE put #-}

-- replicate an operation and build a reversed list of results.
replicateReversed :: (Monad m) => [a] -> Int -> m a -> m [a]
replicateReversed xs 0 _ = return xs
replicateReversed xs n op = op >>= \ x -> replicateReversed (x:xs) (n-1) op

-- single pass to count and reverse list
countAndReverse :: [a] -> (Int, [a])
countAndReverse = cr [] 0 where
    cr l !n (x:xs) = cr (x:l) (n+1) xs
    cr l !n [] = (n, l)


-- note that ((a,b),c) and (a,(b,c)) share serialized structure.
-- So does ((a,b),(c,d)) and (a,(b,c),d), etc.
instance (VCacheable a, VCacheable b) => VCacheable (a,b) where
    get = liftM2 (,) get get
    put (a,b) = do { put a; put b }
    {-# INLINE get #-}
    {-# INLINE put #-}

instance (VCacheable a, VCacheable b, VCacheable c) => VCacheable (a,b,c) where
    get = liftM3 (,,) get get get
    put (a,b,c) = do { put a; put b; put c }
    {-# INLINE get #-}
    {-# INLINE put #-}

instance (VCacheable a, VCacheable b, VCacheable c, VCacheable d) 
    => VCacheable (a,b,c,d) where
    get = liftM4 (,,,) get get get get
    put (a,b,c,d) = do { put a; put b; put c; put d }
    {-# INLINE get #-}
    {-# INLINE put #-}

instance (VCacheable a, VCacheable b, VCacheable c, VCacheable d, VCacheable e) 
    => VCacheable (a,b,c,d,e) where
    get = liftM5 (,,,,) get get get get get
    put (a,b,c,d,e) = do { put a; put b; put c; put d; put e }
    {-# INLINE get #-}
    {-# INLINE put #-}

instance (VCacheable a, VCacheable b, VCacheable c, VCacheable d, VCacheable e
         , VCacheable f) => VCacheable (a,b,c,d,e,f) where
    get = 
        do a <- get; b <- get; c <- get
           d <- get; e <- get; f <- get
           return (a,b,c,d,e,f)
    put (a,b,c,d,e,f) = do { put a; put b; put c; put d; put e; put f }
    {-# INLINE get #-}
    {-# INLINE put #-}

instance (VCacheable a, VCacheable b, VCacheable c, VCacheable d, VCacheable e
         , VCacheable f, VCacheable g) => VCacheable (a,b,c,d,e,f,g) where
    get = 
        do a <- get; b <- get; c <- get
           d <- get; e <- get; f <- get; g <- get
           return (a,b,c,d,e,f,g)
    put (a,b,c,d,e,f,g) = do { put a; put b; put c; put d; put e; put f; put g }
    {-# INLINE get #-}
    {-# INLINE put #-}


