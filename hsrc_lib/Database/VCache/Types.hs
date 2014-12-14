{-# LANGUAGE DeriveDataTypeable
  , ImpredicativeTypes
  , GeneralizedNewtypeDeriving 
  #-}

module Database.VCache.Types
    ( Address, VRef(..), VCache(..)
    , Eph(..), EphMap, Cached(..)
    , VPut(..), VGet(..) 
    , VGetSt(..), vgst_origin
    , VCacheable(..)
    , VRef_, Eph_
    ) where

import Data.Word
import Data.Function (on)
import Data.Typeable
import Data.IORef
import Data.Unique
import Data.IntMap.Strict (IntMap)
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B
import Control.Monad
import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad.Trans.State
import System.Mem.Weak (Weak)
import Database.LMDB.Raw

type Address = Word64

-- | A VRef is an opaque reference to an immutable value stored in an
-- auxiliary address space backed by the filesystem. This allows very
-- large values to be represented without burdening the process heap
-- or Haskell garbage collector. 
--
-- Important Points:
--
-- * Holding a VRef in memory prevents GC of the referenced value.
--   Under the hood, `mkWeakMVar` is used for an ephemeron table.
-- * Values are cached such that multiple derefs in a short period
--   of time will efficienty return the same Haskell value. Caching
--   may be voluntarily bypassed. 
-- * Structure sharing. Within a cache, VRefs are equal iff their 
--   values are equal. VRefs from different caches are never equal.
--
data VRef a = VRef 
    { vref_addr   :: {-# UNPACK #-} !Address            -- address within the cache
    , vref_cache  :: {-# UNPACK #-} !(MVar (Cached a))  -- cached value & weak refs 
    , vref_origin :: !VCache                            -- cache manager for VRef
    } deriving (Typeable)

instance Eq (VRef a) where
    (==) a b = ((==) `on` vref_addr) a b 
            && ((==) `on` (vcache_id . vref_origin)) a b

-- | A VCache represents an auxiliary address space, backed by the
-- filesystem via LMDB. This address space may be much larger than
-- machine RAM without burdening the Haskell garbage collector or
-- causing the system to thrash. 
--
-- Memcaching: frequent operations involve moving values to and from
-- the auxiliary address space:
--
--      vref  :: (Cacheable a) => VCache -> a -> VRef a
--      deref :: VRef a -> a
--
-- These operations are logically pure, and are presented thusly in
-- the API. But they do perform a fair amount of IO under the hood.
-- VCache tracks which VRefs are in memory to avoid garbage collecting
-- any values that might later be dereferenced.
--
-- By default, deref will cache the value for a few seconds, such that
-- values in the working set are held in Haskell memory. This effect 
-- may be avoided by deref'.
--
-- VCache attempts to keep values allocated at the same time near each
-- other in the persistent file. This provides greater cache locality 
-- for related values, at least under the heuristic assumption that 
-- temporal locality corresponds to spatial locality.
-- 
-- Persistence: another main feature of VCache is a simple persistence
-- model, usable as a replacement for acid-state. VCache includes a 
-- table of named roots (the keys are bytestrings) and a simple model
-- to update them transactionally. Transactions are made durable by 
-- explicitly synching the VCache. 
-- 
-- Caveats: A VCache associated with a given filesystem directory may
-- only be loaded by one Haskell process at a time, and only once within
-- a process. The LMDB backing file is not especially portable to systems
-- with different page sizes, endianness, etc..
--
-- Overhead: rough estimate is ~64 bytes for every value added to the
-- database, ~96 bytes for every VRef in memory. I would try to keep
-- VRefs pointing at values at least 64 bytes in size. When persisted, 
-- a VRef requires only 8 bytes (e.g. because no need for GC tracking).
-- 
data VCache = VCache
    { vcache_id         :: !Unique 

    -- LMDB contents. 
    , vcache_db_env     :: !MDB_env
    , vcache_db_values  :: {-# UNPACK #-} !MDB_dbi' -- address → value                      16
    , vcache_db_vroots  :: {-# UNPACK #-} !MDB_dbi' -- address → address                    
    , vcache_db_caddrs  :: {-# UNPACK #-} !MDB_dbi' -- hashval → [address]                  24
    , vcache_db_refcts  :: {-# UNPACK #-} !MDB_dbi' -- address → Word64                     24
    , vcache_db_refct0  :: {-# UNPACK #-} !MDB_dbi' -- address (queue, unsorted?)

    -- Resource Tracking
    , vcache_tracker    :: {-# UNPACK #-} !(IORef EphMap)

    -- Further, I need:
    --   a thread performing writes and incremental GC
    --   a channel to talk to that thread
    }

--
-- Implementation ideas for VCache:
--
--   Use a dedicated thread (via forkOS) for all write transactions.
--
--   Model a simple channel or queue to talk to this thread.
--
--   Merge many writes into one transaction, e.g. over the course
--   of a few milliseconds at a time. This could greatly reduce the
--   synchronization burdens.
--


-- For every VRef we have in memory, we need an ephemeron in a table.
-- This ephemeron table supports structure sharing, caching, and GC.
--
-- I model this ephemeron by use of `mkWeakMVar`.
data Eph a = Eph
    { eph_addr :: {-# UNPACK #-} !Address
    , eph_type :: !TypeRep
    , eph_weak :: {-# UNPACK #-} !(Weak (MVar (Cached a)))
    }
type Eph_ = forall a . Eph a -- forget the value type.
type EphMap = IntMap [Eph_] -- bucket hashmap on address

-- Every VRef has a hole (via MVar) to potentially record a cached
-- value without loading it from a bytestring every time. 
--
-- Besides the value, I also keep a simple bitfield to help the
-- garbage collector decide when to remove a value from the
-- cache. This is treated as a bitfield: initially zero, and 
-- the lowest bit reset to zero whenever we read from the
-- cache (modulo `deref'`). 
data Cached a = Cached 
    { c_value :: !a
    , c_meta  :: {-# UNPACK #-} !Word32
    }


-- | To be utilized with VCache, a value must be serializable as a 
-- simple sequence of bytes and VRefs, and must be Typeable to support
-- caching and tracking at the Haskell layer. Stashing then caching a
-- value should result in an equivalent value. 
--
-- Under the hood, a VRef is effectively recorded as a pair:
--
--    ([VRef],ByteString)
--
-- The VRefs must be loaded in the same order and quentity as emitted
-- (thus, the ByteString should contain hints about when to ask for a
-- VRef). But there is no risk of accidentally confusing bytes from a
-- VRef with bytes from the bytestring. This separation simplifies the
-- VCache garbage collector because it can trace values without knowing
-- their type information, and without any sophisticated parsing.
--
-- The VPut and VGet methods build above Put and Get from Data.Binary.
-- Any Put or Get method for binary strings may be lifted into them. 
-- 
class (Typeable a) => VCacheable a where 
    -- | Stash is a function that will record the value into the VCache
    -- auxiliary store as an ad-hoc sequence of VRefs and binary data. 
    stash :: a -> VPut ()

    -- | Cache will load a value from the auxiliary space into Haskell
    -- memory, returning the value in weak-head normal form. This operation
    -- must load bytes and VRefs in the same quantity and ordering as they
    -- were stashed, and should result in an equivalent value.
    --
    -- Developers should ensure that caching is backwards compatible for
    -- all versions of a type. This might be achieved by recording type
    -- information, or by using a more generic intermediate structure.
    cache :: VGet a

-- VRef without type information. 
type VRef_ = forall a . VRef a

-- | VPut allows a program to emit a sequence of VRefs and binary data.
newtype VPut a = VPut { _vput :: StateT [VRef_] B.PutM a } 
    deriving (Functor, Applicative, Monad)

-- | VGet allows a program to consume a sequence of VRefs and binary
-- data in order to rebuild a complex Haskell value.
newtype VGet a = VGet { _vget :: StateT VGetSt B.Get a }
    deriving (Functor, Applicative, Alternative, Monad, MonadPlus)

-- When performing Get, we'll want to:
--   guarantee the parent VRef is not GC'd.
--   access cached representations of existing VRefs.
--   load a stack of VRef addresses into the resulting value.
data VGetSt = VGetSt
    { vgst_stack  :: ![Address] -- stack of addresses to read as VRefs
    , vgst_source :: !VRef_     -- prevent GC of parent; access VCache
    }

vgst_origin :: VGetSt -> VCache
vgst_origin = vref_origin . vgst_source

