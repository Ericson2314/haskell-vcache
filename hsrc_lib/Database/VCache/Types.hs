{-# LANGUAGE DeriveDataTypeable
  , ImpredicativeTypes
  , GeneralizedNewtypeDeriving 
  #-}

-- Internal file. Lots of types.
module Database.VCache.Types
    ( Address, HashVal
    , VRef(..), Cached(..), VRef_
    , Eph(..), Eph_, EphMap 
    , PVar(..), PVar_
    , PVEph(..), PVEph_, PVEphMap
    , VTx(..)
    , VCache(..)
    , VCRoot(..)
    ) where

import Data.Word
import Data.Function (on)
import Data.Typeable
import Data.IORef
import Data.IntMap.Strict (IntMap)
import Data.Map.Strict (Map)
import Data.ByteString (ByteString)
import Control.Monad
import Control.Monad.STM (STM)
import Control.Applicative
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Monad.Trans.State
import System.Mem.Weak (Weak)
import System.FileLock (FileLock)
import Database.LMDB.Raw

type Address = Word64
type HashVal = Word64

-- | A VRef is an opaque reference to an immutable value stored in an
-- auxiliary address space backed by the filesystem. This allows very
-- large values to be represented without burdening the process heap
-- or Haskell garbage collector. 
--
-- The API involving VRefs is conceptually pure.
--
--     vref   :: (Cacheable a) => VCache -> a -> VRef a
--     deref  :: VRef a -> a
--     region :: VRef a -> VCache
--     copyTo :: VRef a -> VCache -> VRef a
-- 
-- We are moving data around under the hood (via unsafePerformIO),
-- but the client can pretend that VRef is just another value.
-- 
-- Important Points:
--
-- * Holding a VRef in memory prevents GC of the referenced value.
--   Under the hood, `mkWeakMVar` is used for an ephemeron table.
-- * Values are cached such that multiple derefs in a short period
--   of time will efficienty return the same Haskell value. Caching
--   may be voluntarily bypassed. 
-- * Structure sharing. Within a cache, VRefs are equal iff their 
--   values are equal. (VRefs from different caches are not equal.)
--
data VRef a = VRef 
    { vref_addr   :: {-# UNPACK #-} !Address            -- ^ address within the cache
    , vref_cache  :: {-# UNPACK #-} !(MVar (Cached a))  -- ^ cached value & weak refs 
    , vref_region :: !VCRoot                            -- ^ cache manager for this VRef
    } deriving (Typeable)
instance Eq (VRef a) where (==) = (==) `on` vref_cache
type VRef_ = forall a . VRef a -- VRef without type information. 



-- IDEA:
-- 
-- Also support a ZCV 'slice' concept, as a special case involving
-- a zero cache, zero copy value Essentially, this might work like:
--
--  data ZCV = ZCV 
--   { zcv_addr :: {-# UNPACK #-} !Address
--   , zcv_weak :: {-# UNPACK #-} !(MVar ())
--   , zcv_region :: !VCRoot
--   }
--
-- readZCV :: ZCV -> VGet a -> a
--
-- Actually, a possibly more interesting variation is to use `VGet a`
-- directly in the type of a VRef. However, that would hinder Eq type
-- for VRefs, and thus might not be wortwhile.


-- For every VRef we have in memory, we need an ephemeron in a table.
-- This ephemeron table supports structure sharing, caching, and GC.
-- I model this ephemeron by use of `mkWeakMVar`.
data Eph a = Eph
    { eph_addr :: {-# UNPACK #-} !Address
    , eph_type :: !TypeRep
    , eph_weak :: {-# UNPACK #-} !(Weak (MVar (Cached a)))
    }
type Eph_ = forall a . Eph a -- forget the value type.
type EphMap = IntMap [Eph_] --  bucket hash on Address & TypeRep
    -- note: the TypeRep must be included because otherwise a type
    -- that has too many overlapping representations would have very
    -- deep buckets. Alternatively, I could use 
    --
    --     Map Address (Map TypeRep Eph_))
    --
    -- but, for now, I'll assume that GC needs to process the whole
    -- map anyway, and so extracting live addresses should be easy.

-- Every VRef has a hole (via MVar) to potentially record a cached
-- value without loading it from LMDB. A little extra information
-- supports heuristics to decide when to clear the value from memory.
data Cached a = Cached 
    { cached_value  :: a
    , cached_gcbits :: {-# UNPACK #-} !Word16
    }
-- gcbits:
--   bit 0: set 0 when read, set 1 by every GC pass
--   bit 1,2,3,4,5: log scale weight estimate
--   bit 6,7: cache control mode.
--   bits 8..15: for GC internal use.
-- 
-- basic weight heuristic...
--   80 + bytes + 80*refs
--   size heuristic is 2^(N+8)
--   (above max, just use max)
--
-- cache control modes?
-- 
--  0: minimal timeout
--  1: short timeout
--  2: long timeout
--  3: lock into cache
--  
   

-- | A PVar is a persistent variable associated with a VCache and named
-- by a short bytestring. PVars can be manipulated transactionally, and
-- these transactions may also integrate with STM TVars. 
--
--     loadPVarIO :: (VCacheable a) => VCache -> ByteString -> VRef a -> IO (PVar a)
--     loadPVar   :: (VCacheable a) => VCache -> ByteString -> VRef a -> VTxn (PVar a)
--     readPVar   :: PVar a -> VTxn (VRef a)
--     writePVar  :: PVar a -> VRef a -> VTxn ()
--     runVTxn    :: VTxn a -> IO a
--
data PVar a = PVar
    { pvar_content :: !(TVar (VRef a))
    , pvar_name    :: !ByteString -- full name (including path)
    , pvar_region  :: !VCRoot     -- 
    } deriving (Typeable)
instance Eq (PVar a) where (==) = (==) `on` pvar_content
type PVar_ = forall a . PVar a
data PVEph a = PVEph !TypeRep !(Weak (TVar (VRef a)))
type PVEph_ = forall a . PVEph a
type PVEphMap = Map ByteString PVEph_


-- | A VCache represents a filesystem-backed address space.
--
-- This allows an address space much larger than system memory to be
-- efficiently utilized... at least for applications where the working
-- set is much smaller than total storage requirements. Also, VCache
-- supports persistent variables with atomic updates, and thus serves
-- as an alternative to the acid-state package. 
--
-- A given VCache file may only be opened by one process at a time, and
-- only once within that process. This is weakly enforced by lockfile.
--
--
-- It is safe to use more than one VCache object, so long as each is
-- backed by a different file. This might be motivated for similar 
-- roles as a tmpfs - i.e. if you're just using VCache for large 
-- volatile storage that can be deleted when the process finishes. 
-- However, there should rarely be any need to do so. LMDB scales
-- very well.
--
-- Usage Note: If a library or framework uses a VCache, it should 
-- always take a VCache as an argument rather than create its own.
-- This works well with VCache's hierarchical decomposition model,
-- and enables efficient sharing between libraries or frameworks.
--
-- See PVar and VRef for more information.
data VCache = VCache
    { vcache_root :: !VCRoot 
    , vcache_path :: ByteString 
    } deriving (Eq)

data VCRoot = VCRoot
    { vcache_lockfile   :: {-# UNPACK #-} !FileLock -- prevent opening VCache twice

    -- LMDB contents. 
    , vcache_db_env     :: !MDB_env
    , vcache_db_values  :: {-# UNPACK #-} !MDB_dbi' -- address → value
    , vcache_db_vroots  :: {-# UNPACK #-} !MDB_dbi' -- bytes → address 
    , vcache_db_caddrs  :: {-# UNPACK #-} !MDB_dbi' -- hashval → [address]
    , vcache_db_refcts  :: {-# UNPACK #-} !MDB_dbi' -- address → Word64
    , vcache_db_refct0  :: {-# UNPACK #-} !MDB_dbi' -- address (queue, unsorted?)

    , vcache_mem_vrefs  :: {-# UNPACK #-} !(IORef EphMap) -- track VRefs in memory
    , vcache_mem_pvars  :: {-# UNPACK #-} !(IORef PVEphMap) -- track PVars in memory
   

    -- requested weight limit for cached values
    -- , vcache_weightlim  :: {-# UNPACK #-} !(IORef Int)

    -- share persistent variables for safe STM

    -- Further, I need...
    --   a FileLock
    --   a thread performing writes and incremental GC
    --   a channel to talk to that thread
    --   queue of MVars waiting on synchronization/flush.

    }

instance Eq VCRoot where (==) = (==) `on` vcache_lockfile

-- action: write a value to the database.
{-
data WriteVal = WriteVal
    { wv_addr :: {-# UNPACK #-} !Address
    , wv_hash :: {-# UNPACK #-} !HashVal
    , wv_data :: !ByteString
    }
-}





--
-- Behavior of the timer thread:
--   wait for a short period of time
--   writeIORef rfTick True
--   die (new timer thread created every transaction)
--
--   rfTick is used to help prevent latencies from growing
--   too large, especially when someone is waiting on a
--   synchronization. But it also enables combining many
--   transactions.
--
-- Behavior of the main writer thread:
--
--   wait for old readers 
--   create a write transaction
--   create a timer/tick thread
--   select the first unit of work
--   

-- Behavior of the forkOS thread:
--   create a transaction and a callback resource
--   wait on the callback to indicate time to commit
--   commit the transaction in same OS thread, start a new one (unless halted)
--   optional continuation action
--      start a new transaction
--      synchronize to disk
--      report synchronization to anyone waiting on it

--   if a tick has occurred
--     finish current transaction
--     start a new one
--   read the current queue of operations.
--   perform each operation in queue. 
--   aggregate anyone waiting for synch.
--   repeat.
--



-- DESIGN:
--   Using MDB_NOLOCK:
--     No need for a thread to handle the LMDB write mutex.
--
--     Instead, I need to track reader threads and prevent
--     writers from operating while readers exist for two
--     generations back. (Or was it three?)
--
--     Thus: the writer waits on readers, but with double or
--     triple buffering for pipeline concurrency.
--
--  THREADS
--   
--   The writer thread should process a simple command stream
--   consisting primarily of writes.
--
--   Reader transactions may then be created as needed in Haskell
--   threads, mostly to deref values. 
--
--  VREF 
--
--   The 'vref' constructor must interact with the writer. I think
--   there are two options:
--
--     (a) the 'vref' constructor immediately creates a reader to
--         find a match, and only adds to the writer if the value
--         does not already exist. In this case, we already have:
--            the hash value (for content addressing)
--            the bytestring (to create the hash value)
--         we can formulate a 'collection' of writes-to-be-performed,
--     (b) the 'vref' constructor waits on the writer thread, which
--         must still perform a content-addressing hash before storing
--         the value.
--
--   Thus, either way I need a strict bytesting up to forming a hash 
--   value. Unless I'm going to hash just a fraction of the data, I 
--   should probably select (a).
--
--  PVARS
--
--   With PVars, it seems feasible to simply send a 'write batch' after
--   every transaction, with an optional sync request. I think this will
--   be much easier than working with the VRefs. I will need to ensure 
--   that VRefs are properly written before PVar's current value.
--
--   I can probably run a quick check to test whether each PVar has changed.
--
--   Simple command-pattern or queue to talk to this thread.
--   Lots of writes merge into one larger transaction. 
--   Synchronization at lower rate than writes, 
--     except where otherwise requested.
--
--   Avoid waiting on this write thread for normal interactions.
--     can't wait on current writer for a new VRef address?
--
--     In this case, I'll need to track pending writes, so 
--     that I can pick the Address before storing anything to
--     it. And I might need to double-buffer this.
--
--   No waiting on the write thread for normal interactions.
--   Write thread can write STM-layer transactional snapshots.
--   May wait on writer only to ensure durable transactions.
--



-- | The VTx transactions build upon STM, and allow use of arbitrary
-- PVars in addition to STM resources. 
--
-- The PVars may originate from multiple VCaches. However, in that
-- case atomicity cannot be guaranteed because the program might crash
-- between updating two VCaches. Transactions are atomic only if at 
-- most one VCache is involved.
--
-- Durability is optional. Asking for a durable transaction just means
-- you wait for all the VCaches to finish writing and synching before
-- you return from the transaction request. If you don't write, the
-- durable transactions will not perform any synch.
-- 
newtype VTx a = VTx { _vtx :: StateT [PVar_] STM a }
    deriving (Monad, Functor, Applicative, Alternative, MonadPlus)
    -- Internally, I'm just tracking a list of PVars written. 
    --
    -- When the transaction completes, this list will be used to
    -- alert the writer threads. There may be an optional further
    -- wait for all writes to complete.






