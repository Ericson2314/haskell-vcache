
-- primary implementation of VCache (might break up later)
module Database.VCache.Impl 
    ( mvref
    ) where

import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Database.VCache.Types

-- | Move a VRef into a target destination space. This will exit fast
-- if the VRef is already in the target location, otherwise performs a
-- deep copy. 
--
-- In general, developers should try to avoid moving values between 
-- caches. This is more efficient than parsing and serializing values,
-- but is still expensive and shouldn't happen by accident. Developers
-- using more than one VCache (a rather unusual scenario) should have
-- a pretty good idea about the dataflow between them to minimize this
-- operation.
--
mvref :: VSpace -> VRef a -> VRef a
mvref sp ref =
    if (sp == vref_space ref) then ref else
    _deepCopy sp ref

_deepCopy :: VSpace -> VRef a -> VRef a
_deepCopy dst src = error "todo: mvref"

