
-- This module describes a variation of Data.Binary.Get optimized for use
-- with VCache. In particular, VCache will always provide data to Get as
-- an MDB_val, i.e. just a pointer and a size. I wish to minimize copies 
-- on this data.
--
-- In this case, our basic input is an MDB_val, which is just a Ptr Word8 
-- together with a size value. However, we must be careful because the
-- 
module Database.VCache.Get
    ( Get, GetFixed(..)
    , requireSize, isolate 
    ) where

import Control.Monad.Trans.State.Strict
import Data.Word (Word8)
import Foreign.Ptr
import Foreign.Storable

type Ptr8 = Ptr Word8
type Loc = Ptr8
type Lim = Ptr8


-- 
unsafeGetWord8 :: Get Word8
unsafeGetWord8 = G (_ p -> Recv <$> peek p <*> ( 



