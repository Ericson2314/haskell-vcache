{-# LANGUAGE DeriveDataTypeable, ExistentialQuantification, GeneralizedNewtypeDeriving #-}

-- Internal file. Lots of types. 
-- Lots of coupling... but VCache is just one module.
module Database.VCache.Types
    ( Address
    , VRef(..), Cached(..), VRef_(..)
    , Eph(..), Eph_(..), EphMap 
    , PVar(..), PVar_(..)
    , PVEph(..), PVEphMap
    , VTx(..)
    , VCache(..)
    , VSpace(..)
    , VPut(..), VPutS(..), VPutR(..)
    , VGet(..), VGetS(..), VGetR(..)
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
import Foreign.Ptr

type Address = Word64 -- with '0' as special case

-- | A VRef is an opaque reference to an immutable value stored in an
-- auxiliary address space backed by the filesystem. This allows very
-- large values to be represented without burdening the process heap
-- or Haskell garbage collector. 
--
-- The API involving VRefs is conceptually pure.
--
--     vref     :: (VCacheable a) => VSpace -> a -> VRef a
--     deref    :: (VCacheable a) => VRef a -> a
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
--   may be bypassed or controlled by developers.
-- * Structure sharing. Within a cache, VRefs are equal iff their 
--   values are equal. (VRefs from different caches are not equal.)
--
data VRef a = VRef 
    { vref_addr   :: {-# UNPACK #-} !Address            -- ^ address within the cache
    , vref_cache  :: {-# UNPACK #-} !(MVar (Cached a))  -- ^ cached value & weak refs 
    , vref_space  :: !VSpace                            -- ^ cache manager for this VRef
    , vref_parse  :: !(VGet a)                          -- ^ parser for this VRef
    } deriving (Typeable)
instance Eq (VRef a) where (==) = (==) `on` vref_cache
data VRef_ = forall a . VRef_ !(VRef a) -- VRef without type information.

-- idea: vrefNear :: (VCacheable a) => VRef b -> a -> VRef a

-- For every VRef we have in memory, we need an ephemeron in a table.
-- This ephemeron table supports structure sharing, caching, and GC.
-- I model this ephemeron by use of `mkWeakMVar`.
data Eph a = Eph
    { eph_addr :: {-# UNPACK #-} !Address
    , eph_type :: !TypeRep
    , eph_weak :: {-# UNPACK #-} !(Weak (MVar (Cached a)))
    }
data Eph_ = forall a . Eph_ (Eph a) -- forget the value type.
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
   

-- | A PVar is a persistent variable associated with a VCache. PVars are
-- second class: named by bytestrings in a filesystem-like namespace,
-- but not themselves cacheable.
--
--     loadPVarIO :: (VCacheable a) => VCache -> ByteString -> a -> IO (PVar a)
--     loadPVar   :: (VCacheable a) => VCache -> ByteString -> a -> VTx (PVar a)
--     readPVarIO :: PVar a -> IO a
--     readPVar   :: PVar a -> VTx a
--     writePVar  :: PVar a -> a -> VTx ()
--     runVTx     :: VTx a -> IO a
--
-- The PVar type `a` does not use structure sharing or memcaching. The full
-- value is loaded into Haskell memory, and updated by writer transactions.
-- To take full advantage of VCache, developers must use VRefs in type `a`,
-- and keep `a` itself relatively small.
--
-- Note: PVar names should be short. Names smaller than 128 characters will
-- be used directly, but larger names may be hashed and truncated, at risk
-- of accidental collisions and security holes if clients control names. For
-- most use-cases, this should be a non-issue.
--
data PVar a = PVar
    { pvar_content :: !(TVar a)
    , pvar_name    :: !ByteString -- full name
    , pvar_space   :: !VSpace     -- 
    , pvar_write   :: !(a -> VPut ())
    } deriving (Typeable)
instance Eq (PVar a) where (==) = (==) `on` pvar_content
data PVar_ = forall a . PVar_ !(PVar a)
data PVEph = forall a . PVEph !TypeRep {-# UNPACK #-} !(Weak (TVar a))
type PVEphMap = Map ByteString PVEph

--     dropPVar   :: VCache -> ByteString -> VTx ()


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
    --   a thread performing writes and incremental GC
    --   a channel to talk to that thread
    --   queue of MVars waiting on synchronization/flush.

    }

instance Eq VSpace where (==) = (==) `on` vcache_lockfile

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

-- | The VTx transactions build upon STM, and allow use of arbitrary
-- PVars in addition to STM resources. 
--
-- VTx transactions on a single VCache can support the full atomic,
-- consistent, isolated, and durable (ACID) properties. However, 
-- durability is optional, and non-durable transactions may observe 
-- data that has been committed but is not fully persisted to disk.
-- Essentially, durability involves an extra wait (before returning)
-- for `fsync` to disk. Only durable transactions will wait.
--
-- A transaction involving PVars from multiple VCache objects is
-- possible, but full ACID is not supported: if the Haskell process
-- crashes between persisting the different underlying files, they
-- will become mutually inconsistent (though will still have internal
-- consistency).
-- 
-- VCache will aggregate multiple transactions that occur around
-- the same time, amortizing synchronization costs for bursts of
-- updates (common in many domains).
-- 
newtype VTx a = VTx { _vtx :: StateT WriteList STM a }
    deriving (Monad, Functor, Applicative, Alternative, MonadPlus)
    -- basically, just an STM transaction that additionally tracks
    -- writes to PVars involving many VCache resources. This list 
    -- is delivered to the writer threads only if the transaction
    -- commits. Serialization is lazy to better support batching. 

type WriteList  = [(VSpace, WriteBatch)]           
type WriteBatch = Map ByteString (VPut ()) 

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
    , vput_children :: ![VRef_]
    , vput_buffer   :: !(IORef PtrIni)
    , vput_target   :: {-# UNPACK #-} !PtrLoc
    , vput_limit    :: {-# UNPACK #-} !PtrEnd
    }
    -- note: vput_buffer is an IORef mostly to simplify error handling.
    --  On error, we'll need to free the buffer. However, it may be 
    --  reallocated many times during serialization of a large value,
    --  so we need easy access to the final value.

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


