{-# LANGUAGE DeriveDataTypeable, ExistentialQuantification, GeneralizedNewtypeDeriving #-}

-- Internal file. Lots of types. Lots of coupling.
module Database.VCache.Types
    ( Address, isVRefAddr, isPVarAddr
    , VRef(..), Cache(..)
    , Eph(..), EphMap 
    , PVar(..), RDV
    , PVEph(..), PVEphMap
    , VTx(..), TxW(..)
    , VCache(..)
    , VSpace(..)
    , VCacheStats(..)
    , VPut(..), VPutS(..), VPutR(..), PutChild(..)
    , VGet(..), VGetS(..), VGetR(..)
    , VCacheable(..)
    , Allocator(..), AllocFrame(..), Allocation(..)
    ) where

import Data.Bits
import Data.Word
import Data.Function (on)
import Data.Typeable
import Data.IORef
import Data.IntMap.Strict (IntMap)
-- import Data.Map.Strict (Map)
import Data.ByteString (ByteString)
import Control.Monad
import Control.Monad.STM (STM)
import Control.Applicative
-- import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Monad.Trans.State
import System.Mem.Weak (Weak)
import System.FileLock (FileLock)
import Database.LMDB.Raw
import Foreign.Ptr

import Database.VCache.RWLock

-- | An address in the VCache address space
type Address = Word64 -- with '0' as special case

-- | VRefs and PVars are divided among odds and evens.
isVRefAddr, isPVarAddr :: Address -> Bool
isVRefAddr addr = (0 == (1 .&. addr))
isPVarAddr = not . isVRefAddr
{-# INLINE isVRefAddr #-}
{-# INLINE isPVarAddr #-}

-- | A VRef is an opaque reference to an immutable value stored in an
-- auxiliary address space backed by the filesystem (via LMDB). VRef 
-- allows very large values to be represented without burdening the 
-- process heap or Haskell garbage collector. The assumption is that
-- the very large value doesn't need to be in memory all at once.
--
-- The API involving VRefs is conceptually pure.
--
--     vref     :: (VCacheable a) => VSpace -> a -> VRef a
--     deref    :: VRef a -> a
-- 
-- Construction must serialize the value, hash the serialized form
-- (for structure sharing), and write it into the backend. Deref will
-- read the serialized form, parse the value back into Haskell memory,
-- and cache it locally. Very large values and domain models are 
-- supported by allowing VRefs and PVars to themselves be cacheable.
--
-- Developers have some ability to bypass or influence cache behavior.
--
data VRef a = VRef 
    { vref_addr   :: {-# UNPACK #-} !Address            -- ^ address within the cache
    , vref_cache  :: {-# UNPACK #-} !(IORef (Cache a))  -- ^ cached value & weak refs 
    , vref_space  :: !VSpace                            -- ^ cache manager for this VRef
    , vref_parse  :: !(VGet a)                          -- ^ parser for this VRef
    } deriving (Typeable)
instance Eq (VRef a) where (==) = (==) `on` vref_cache

-- For every VRef we have in memory, we need an ephemeron in a table.
-- This ephemeron table supports structure sharing, caching, and GC.
-- I model this ephemeron by use of `mkWeakMVar`.
data Eph = forall a . Eph
    { eph_addr :: {-# UNPACK #-} !Address
    , eph_type :: !TypeRep
    , eph_weak :: {-# UNPACK #-} !(Weak (IORef (Cache a)))
    }
type EphMap = IntMap [Eph] -- bucket hashmap on Address & TypeRep
    -- type must be part of hash, or we'll have too many collisions
    -- where values tend to overlap in representation, e.g. the 
    -- 'empty' values.

-- Every VRef contains its own cache. (Thus, there is no extra lookup
-- overhead to test the cache, and this simplifies interaction with GC). 
-- The cache includes an integer as a bitfield to describe mode.
data Cache a 
        = NotCached 
        | Cached a {-# UNPACK #-} !Word16

--
-- cache bitfield for mode:
--   bit 0..1: cache control mode.
--     no timeout, short timeout, long timeout, locked
--   bit 2..6: heuristic weight, log scale.
--     bytes + 80*(refs+1)
--     size heuristic is 2^(N+8)
--   bits 7..15:
--     if locked, count of locks (max val = inf).
--     otherwise, touch counter for timing by GC
--

-- | A PVar is a mutable variable backed by VCache, and which may be
-- referenced by cached values. PVars support ACID transactions like
-- TVars, and may be used together with TVars.
--
-- Unlike VRefs, PVars are not cached. They should be treated as in
-- memory variables, at least after the first read. Their first read
-- is lazy, such that a PVar is not parsed or loaded if not used. A
-- PVar must contain VRefs to support caching or structure sharing.
--
-- In addition to anonymous PVars, VCache supports named global roots
-- for persistence. Further, a simplistic filesystem-like structure 
-- enables subprograms to each have a fresh namespace. Named PVars are
-- the foundation for persistence. 
-- 
-- Developers must be careful with respect to cyclic references among
-- PVars. The VCache garbage collector uses reference counting because
-- it scales nicely to very large content, but cyclic references will
-- stymie this simplistic algorithm. Cycles must be avoided or broken
-- to avoid leaking disk space.
--
data PVar a = PVar
    { pvar_addr  :: {-# UNPACK #-} !Address
    , pvar_data  :: {-# UNPACK #-} !(TVar (RDV a))
    , pvar_write :: !(a -> VPut ())
    , pvar_space :: !VSpace
    } deriving (Typeable)
instance Eq (PVar a) where (==) = (==) `on` pvar_data

-- ephemeron table for PVars.
data PVEph = forall a . PVEph
    { pveph_addr :: {-# UNPACK #-} !Address
    , pveph_type :: !TypeRep
    , pveph_weak :: {-# UNPACK #-} !(Weak (TVar (RDV a)))
    } 
type PVEphMap = IntMap [PVEph]

-- Left iff not read.
type RDV a = Either (VGet a) a


-- | VCache supports a filesystem-backed address space with persistent 
-- variables. The address space can be much larger than system memory, 
-- but the active working set at any given moment should be smaller than
-- system memory. The persistent variables are named by bytestring, and
-- support a filesystem directory-like structure to easily decompose a
-- persistent application into persistent subprograms.
--
-- See VSpace, VRef, and PVar for more information.
data VCache = VCache
    { vcache_space :: !VSpace 
    , vcache_path  :: !ByteString
    } deriving (Eq)

-- | VSpace is the abstract address space used by VCache. VSpace allows
-- construction of VRef values, and may be accessed via VRef or VCache.
-- Unlike the whole VCache object, VSpace does not permit construction of
-- PVars. From the client's perspective, VSpace is just an opaque handle.
data VSpace = VSpace
    { vcache_lockfile   :: {-# UNPACK #-} !FileLock -- block concurrent use of VCache file

    -- LMDB contents. 
    , vcache_db_env     :: !MDB_env
    , vcache_db_memory  :: {-# UNPACK #-} !MDB_dbi' -- address → value
    , vcache_db_vroots  :: {-# UNPACK #-} !MDB_dbi' -- path → address 
    , vcache_db_caddrs  :: {-# UNPACK #-} !MDB_dbi' -- hashval → [address]
    , vcache_db_refcts  :: {-# UNPACK #-} !MDB_dbi' -- address → Word64
    , vcache_db_refct0  :: {-# UNPACK #-} !MDB_dbi' -- address → ()
    , vcache_db_rwlock  :: !RWLock -- replace gap left by MDB_NOLOCK
    , vcache_mem_vrefs  :: {-# UNPACK #-} !(IORef EphMap) -- track VRefs in memory
    , vcache_mem_pvars  :: {-# UNPACK #-} !(IORef PVEphMap) -- track PVars in memory
    , vcache_allocator  :: !(IORef Allocator) -- allocate new VRefs and PVars

    -- requested weight limit for cached values
    -- , vcache_weightlim  :: {-# UNPACK #-} !(IORef Int)

    -- share persistent variables for safe STM

    -- Further, I need...
    --   log or queue of 'new' vrefs and pvars, 
    --     including those still being written
    --   semaphore to block writer on older readers
    --   a thread performing writes and incremental GC
    --   a channel to talk to that thread
    --   queue of MVars waiting on synchronization/flush.

    }

instance Eq VSpace where (==) = (==) `on` vcache_lockfile


-- | the allocator 
--
-- The goal of the allocator is to support developers in adding new
-- VRefs or PVars to the database. The main challenge, here, is to
-- ensure that VRefs are never replicated.
--
-- To that end, we must track recently written VRefs. If a reader
-- tries to find the VRef in the database but fails, it must then
-- test if some other thread has recently allocated it before any
-- attempt to do so itself. 
--
-- How recently? Well, based on the frame buffer concurrency, we 
-- know that while the writer is outputting frame N, readers may
-- be operating on the database from frames N-1 and N-2. Further,
-- we'll have new inputs arriving for frame N+1. So, we need to
-- keep at least three frames of 'recent allocations' for the 
-- N-2 readers. This assumes we will hold RWLock whenever we try
-- to construct a VRef.
--
-- So, three recent frames is all we need. 
--
-- Note: allocations are processed separately from PVar updates.
-- A goal here is to take advantage of the fast MDB_APPEND mode.
--
data Allocator = Allocator
    { alloc_addr :: {-# UNPACK #-} !Address -- next address
    , alloc_frm_next :: !AllocFrame -- frame N+1 (next step)
    , alloc_frm_curr :: !AllocFrame -- frame N   (curr step)
    , alloc_frm_prev :: !AllocFrame -- frame N-1 (prev step)
    }

data AllocFrame = AllocFrame 
    { alloc_list :: [Allocation]        -- all data
    , alloc_vals :: IntMap [Allocation] -- hashmap (for VRefs only)
    }

data Allocation = Alloc 
    { alloc_hash :: {-# UNPACK #-} !ByteString  -- hash value (or empty)
    , alloc_size :: {-# UNPACK #-} !Int
    , alloc_data :: {-# UNPACK #-} !Ptr8
    , alloc_deps :: [PutChild]
    , alloc_dest :: {-# UNPACK #-} !Address
    }

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
--     generations back. (Or was it three? Need to review
--     LMDB code.)
--
--     Thus: the writer waits on readers, but with double or
--     triple buffering for concurrency.
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
--   With PVars, it seems feasible to simply deliver a 'write batch' for
--   every transaction, with an optional sync request. Multiple batches
--   involving one PVar can be combined into a single write at the LMDB
--   layer. For consistency, I'll need to track what each transaction has
--   written to the PVar, rather than using a separate read on the PVars.
--
--   PVars will use STM variables under the hood, allowing interaction with
--   other STM variables at runtime. There is also no waiting on the write
--   thread unless the PVar transaction must be durable.
--

-- | Miscellaneous statistics for a VCache instance. These can be 
-- computed at runtime, though they aren't guaranteed to be atomic
-- or consistent. 
data VCacheStats = VCacheStats
        { vcstat_file_size      :: {-# UNPACK #-} !Int  -- ^ estimated database file size (in bytes)
        , vcstat_vref_count     :: {-# UNPACK #-} !Int  -- ^ number of immutable values in the database
        , vcstat_pvar_count     :: {-# UNPACK #-} !Int  -- ^ number of mutable PVars in the database
        , vcstat_root_count     :: {-# UNPACK #-} !Int  -- ^ number of named roots (a subset of PVars)
        , vcstat_mem_vref       :: {-# UNPACK #-} !Int  -- ^ number of VRefs in Haskell process memory
        , vcstat_mem_pvar       :: {-# UNPACK #-} !Int  -- ^ number of PVars in Haskell process memory
        , vcstat_alloc_pos      :: {-# UNPACK #-} !Address -- ^ address to be used by allocator
        } deriving (Show, Ord, Eq)


-- | The VTx transactions allow atomic updates to both PVars and STM
-- resources, providing a simple basis for consistency between the
-- persistent and ephemeral resources.
--
-- Durability is optional: when a VTx transaction completes, a batch
-- of updates will be commited to disk by a background thread. Many
-- transactions from around the same time will be aggregated by the
-- writer, to amortize synchronization costs. A durable transaction
-- will wait until the writer signals that everything is backed by
-- the filesystem. A non-durable transaction will not wait, but may
-- read and write values without guarantee that they are backed to
-- disk.
-- 
newtype VTx a = VTx { _vtx :: StateT WriteLog STM a }
    deriving (Monad, Functor, Applicative, Alternative, MonadPlus)
    -- basically, just an STM transaction that additionally tracks
    -- writes to PVars, potentially across many VCache resources.
    -- List is delivered to the writer threads only if the transaction
    -- commits. Serialization is lazy to better support batching. 

type WriteLog  = [TxW]
data TxW = forall a . TxW !(PVar a) a
    -- note: PVars should not be GC'd before written to disk. An
    -- alternative is that `loadPVar` waits on active writes, or
    -- accesses the global write log. But it's easiest to simply
    -- not GC until finished writing.

type Ptr8 = Ptr Word8
type PtrIni = Ptr8
type PtrEnd = Ptr8
type PtrLoc = Ptr8

-- | VPut represents an action that serializes a value as a string of
-- bytes and references to smaller values. Very large values can be
-- represented as a composition of other, slightly less large values.
--
-- Under the hood, VPut simply grows an array as needed (via realloc),
-- using an exponential growth model to limit amortized costs. Use 
-- `reserve` to allocate enough bytes.
newtype VPut a = VPut { _vput :: VPutS -> IO (VPutR a) }
data VPutS = VPutS 
    { vput_space    :: !VSpace 
    , vput_children :: ![PutChild] -- ^ addresses written
    , vput_buffer   :: !(IORef PtrIni) -- ^ current buffer for easy free
    , vput_target   :: {-# UNPACK #-} !PtrLoc -- ^ location within buffer
    , vput_limit    :: {-# UNPACK #-} !PtrEnd -- ^ current limit for input
    }
    -- note: vput_buffer is an IORef mostly to simplify error handling.
    --  On error, we'll need to free the buffer. However, it may be 
    --  reallocated many times during serialization of a large value,
    --  so we need easy access to the final value.
data PutChild = forall a . PutChild (Either (PVar a) (VRef a))

data VPutR r = VPutR
    { vput_result :: r
    , vput_state  :: !VPutS
    }

instance Functor VPut where 
    fmap f m = VPut $ \ s -> 
        _vput m s >>= \ (VPutR r s') ->
        return (VPutR (f r) s')
    {-# INLINE fmap #-}
instance Applicative VPut where
    pure = return
    (<*>) = ap
    {-# INLINE pure #-}
    {-# INLINE (<*>) #-}
instance Monad VPut where
    fail msg = VPut (\ _ -> fail ("VCache.VPut.fail " ++ msg))
    return r = VPut (\ s -> return (VPutR r s))
    m >>= k = VPut $ \ s ->
        _vput m s >>= \ (VPutR r s') ->
        _vput (k r) s'
    m >> k = VPut $ \ s ->
        _vput m s >>= \ (VPutR _ s') ->
        _vput k s'
    {-# INLINE return #-}
    {-# INLINE (>>=) #-}
    {-# INLINE (>>) #-}


-- | VGet represents an action that parses a value from a string of bytes
-- and values. Parser combinators are supported, and are of recursive
-- descent in style. It is possible to isolate a 'get' operation to a subset
-- of values and bytes, requiring a parser to consume exactly the requested
-- quantity of content.
newtype VGet a = VGet { _vget :: VGetS -> IO (VGetR a) }
data VGetS = VGetS 
    { vget_children :: ![Address]
    , vget_target   :: {-# UNPACK #-} !PtrLoc
    , vget_limit    :: {-# UNPACK #-} !PtrEnd
    , vget_space    :: !VSpace
    }
data VGetR r 
    = VGetR r !VGetS
    | VGetE String


instance Functor VGet where
    fmap f m = VGet $ \ s ->
        _vget m s >>= \ c ->
        return $ case c of
            VGetR r s' -> VGetR (f r) s'
            VGetE msg -> VGetE msg 
    {-# INLINE fmap #-}
instance Applicative VGet where
    pure = return
    (<*>) = ap
    {-# INLINE pure #-}
    {-# INLINE (<*>) #-}
instance Monad VGet where
    fail msg = VGet (\ _ -> return (VGetE msg))
    return r = VGet (\ s -> return (VGetR r s))
    m >>= k = VGet $ \ s ->
        _vget m s >>= \ c ->
        case c of
            VGetE msg -> return (VGetE msg)
            VGetR r s' -> _vget (k r) s'
    m >> k = VGet $ \ s ->
        _vget m s >>= \ c ->
        case c of
            VGetE msg -> return (VGetE msg)
            VGetR _ s' -> _vget k s'
    {-# INLINE fail #-}
    {-# INLINE return #-}
    {-# INLINE (>>=) #-}
    {-# INLINE (>>) #-}
instance Alternative VGet where
    empty = mzero
    (<|>) = mplus
    {-# INLINE empty #-}
    {-# INLINE (<|>) #-}
instance MonadPlus VGet where
    mzero = fail "mzero"
    mplus f g = VGet $ \ s ->
        _vget f s >>= \ c ->
        case c of
            VGetE _ -> _vget g s
            r -> return r
    {-# INLINE mzero #-}
    {-# INLINE mplus #-}


-- | To be utilized with VCache, a value must be serializable as a 
-- simple sequence of binary data and child VRefs. Also, to put then
-- get a value must result in equivalent values. Further, values are
-- Typeable to support memory caching of values loaded.
-- 
-- Under the hood, structured data is serialized as the pair:
--
--    (ByteString,[Either VRef PVar])
--
-- Developers must ensure that `get` on the serialization from `put` 
-- returns the same value. And `get` must be backwards compatible.
-- Developers should consider version wrappers, cf. SafeCopy package.
-- 
class (Typeable a) => VCacheable a where 
    -- | Serialize a value as a stream of bytes and value references. 
    put :: a -> VPut ()

    -- | Parse a value from its serialized representation into memory.
    get :: VGet a



