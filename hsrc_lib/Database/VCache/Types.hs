{-# LANGUAGE DeriveDataTypeable, ExistentialQuantification, GeneralizedNewtypeDeriving #-}

-- Internal file. Lots of types. Lots of coupling.
module Database.VCache.Types
    ( Address, isVRefAddr, isPVarAddr
    , VRef(..), Cache(..), CacheMode(..)
    , Eph(..), EphMap 
    , PVar(..), RDV(..)
    , PVEph(..), PVEphMap
    , VTx(..), VTxState(..), TxW(..), VTxBatch(..)
    , VCache(..), VSpace(..)
    , VPut(..), VPutS(..), VPutR(..)
    , PutChild(..), putChildAddr
    , VGet(..), VGetS(..), VGetR(..)
    , VCacheable(..)
    , Allocator(..), AllocFrame(..), Allocation(..)
    , Writes(..), WriteLog, WriteCt(..)

    -- a few utilities
    , allocFrameSearch
    , withRdOnlyTxn
    , withByteStringVal
    , getVTxSpace
    , markForWrite
    , liftSTM

    , mkVRefCache, cacheWeight, cacheModeBits, touchCache
    ) where

import Data.Bits
import Data.Word
import Data.Function (on)
import Data.Typeable
import Data.IORef
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as BSI
import Control.Monad
import Control.Monad.Trans.Class (lift)
import Control.Monad.STM (STM)
import Control.Applicative
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Monad.Trans.State.Strict
import Control.Exception (bracket)
import System.Mem.Weak (Weak)
import System.FileLock (FileLock)
import Database.LMDB.Raw
import Foreign.Ptr
import Foreign.ForeignPtr (withForeignPtr)

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
-- process heap or Haskell garbage collector. 
--
-- The API involving VRefs is conceptually pure.
--
-- > vref  :: (VCacheable a) => VSpace -> a -> VRef a
-- > deref :: VRef a -> a
--
-- Each VRef acts as a pointer to the underlying data, which may be
-- dereferenced. When dereferenced, the value will typically remain
-- cached in memory for a short while, on the assumption that values
-- part of the active working set will be deref'd frequently. Cache
-- behavior may be bypassed or controlled with variations on deref.
--
-- Large data structures are possible because VRefs themselves are
-- serializable as part of modeling large data structures. Tree-like
-- data can be modeled, and only the active nodes from the tree need
-- be loaded into memory. 
--
-- VRefs implicitly support structure sharing. Two values with the
-- same serialized form will use the same VRef address. Developers
-- may leverage this feature, e.g. directly compare VRefs to test
-- for structural equality, or support memoization or interning. 
--
-- VRefs are read-optimized, with the assumption that you will 
-- deref more frequently than you will construct vrefs. However,
-- constructing VRefs is still reasonably efficient. Unless the
-- assumption breaks down in an extreme way, VCache is probably
-- okay.
--
data VRef a = VRef 
    { vref_addr   :: {-# UNPACK #-} !Address            -- ^ address within the cache
    , vref_cache  :: {-# UNPACK #-} !(IORef (Cache a))  -- ^ cached value & weak refs 
    , vref_space  :: !VSpace                            -- ^ virtual address space for VRef
    , vref_parse  :: !(VGet a)                          -- ^ parser for this VRef
    } deriving (Typeable)
instance Eq (VRef a) where (==) = (==) `on` vref_cache
instance Show (VRef a) where showsPrec _ v = showString "VRef#" . shows (vref_addr v)

-- For every VRef we have in memory, we need an ephemeron in a table.
-- This ephemeron table supports structure sharing, caching, and GC.
-- I model this ephemeron by use of `mkWeakMVar`.
data Eph = forall a . Eph 
    { eph_addr  :: {-# UNPACK #-} !Address
    , eph_type  :: !TypeRep
    , eph_cache :: {-# UNPACK #-} !(Weak (IORef (Cache a)))
    } 
type EphMap = Map Address (Map TypeRep Eph)
    -- Address is at the top layer of the map, here, because that greatly
    -- simplifies the garbage collector: it can simply check whether an 
    -- address is in use. The TypeRep is also necessary because a single
    -- Address can (via structure sharing at the serialization layer) have
    -- many types.

-- Every VRef contains its own cache. (Thus, there is no extra lookup
-- overhead to test the cache, and this simplifies interaction with GC). 
-- The cache includes an integer as a bitfield to describe mode.
data Cache a 
        = NotCached 
        | Cached a {-# UNPACK #-} !Word16

--
-- cache bitfield for mode:
--   bit 0..4: heuristic weight, log scale
--     weight = bytes + 80 * (deps + 1)
--     log scale: 2^(N+7), max N=31
--   bits 5..6: heuristic priority 
--   bit 7: touch; marked 0 on deref
--   bits 8..15: for use by cache manager
--


-- | Cache modes are used when deciding, heuristically, whether to
-- clear a value from cache. The modes don't have precise meaning,
-- but there is a general intention: higher numbered modes indicate
-- that VCache should hold onto a value for longer or with greater
-- priority.
--
-- The default for vref and deref is CacheMode1. Use of vrefc or 
-- derefc may indicate other modes. Cache mode is monotonic; if
-- the same VRef is deref'd with two different modes, the higher
-- mode will be preserved.
--
-- Note: Regardless of mode, a VRef that is fully GC'd from the
-- Haskell layer will ensure any cached content is also GC'd.
-- 
data CacheMode
        = CacheMode0
        | CacheMode1
        | CacheMode2
        | CacheMode3
        deriving (Eq, Ord, Show)

cacheModeBits :: CacheMode -> Word16
cacheModeBits CacheMode0 = 0
cacheModeBits CacheMode1 = 1 `shiftL` 5
cacheModeBits CacheMode2 = 2 `shiftL` 5
cacheModeBits CacheMode3 = 3 `shiftL` 5


-- | clear bit 7, and adjust cache mode monotonically.
touchCache :: CacheMode -> Word16 -> Word16
touchCache cm w =
    let cb' = (w .&. 0x60) `max` cacheModeBits cm in
    (w .&. 0xff1f) .|. cb'
{-# INLINE touchCache #-}

-- | mkVRefCache val weight
mkVRefCache :: a -> Int -> CacheMode -> Cache a
mkVRefCache val w cm = Cached val cw where
    cw = m .|. cs 0 128
    cs r k = if ((k > w) || (r == 0x1f)) then r else cs (r+1) (k*2)
    m = cacheModeBits cm

-- | cacheWeight nBytes nDeps
cacheWeight :: Int -> Int -> Int
cacheWeight nBytes nDeps = nBytes + (80 * (nDeps + 1))

-- | A PVar is a mutable variable backed by VCache. PVars can be read
-- or updated transactionally (see VTx), and may store by reference
-- as part of domain data (see VCacheable). PVars are often anonymous,
-- though named root PVars provide a basis for persistence.
--
-- When loaded from disk, PVar contents are lazily read into Haskell
-- memory when first needed. PVar contents are not cached, i.e. they
-- will remain in Haskell memory for as long as the PVar does. If a
-- PVar is fully GC'd from the Haskell layer, it may again be lazily
-- loaded.
--
-- It is not the case that every write to a PVar results in a write to
-- disk: if a PVar is updated at a higher frequency than the writer
-- thread processes the updates, intermediate values will be skipped. 
-- High frequency or bursty updates are not a big performance concern.
--
-- Programmers must be careful with regards to cyclic references among
-- PVars. The VCache garbage collector uses reference counting, which
-- scales nicely but will leak cyclic data structures. Cycles are not
-- forbidden. But programmers that use cycles must be careful to break
-- cycles when done with them. Cycles should always be reachable from
-- named root PVars, otherwise they will leak upon crash.
--
-- Note: PVars should never contain undefined or error values, or any
-- value that cannot be strictly serialized by a VCacheable instance.
--
data PVar a = PVar
    { pvar_addr  :: {-# UNPACK #-} !Address
    , pvar_data  :: {-# UNPACK #-} !(TVar (RDV a))
    , pvar_write :: !(a -> VPut ())
    , pvar_space :: !VSpace -- ^ virtual address space for PVar
    } deriving (Typeable)
instance Eq (PVar a) where (==) = (==) `on` pvar_data
instance Show (PVar a) where showsPrec _ pv = showString "PVar#" . shows (pvar_addr pv)

-- ephemeron table for PVars.
data PVEph = forall a . PVEph 
    { pveph_addr :: {-# UNPACK #-} !Address
    , pveph_type :: !TypeRep
    , pveph_data :: {-# UNPACK #-} !(Weak (TVar (RDV a)))
    }
type PVEphMap = Map Address PVEph

-- I need some way to force an evaluation when a PVar is first
-- read, i.e. in order to load the initial value, without forcing
-- on every read. For the moment, I'm using a simple type wrapper.
data RDV a = RDV a

-- | VCache supports a filesystem-backed address space plus a set of
-- persistent, named root variables that can be loaded from one run 
-- of the application to another. VCache supports a simple filesystem
-- like model to resist namespace collisions between named roots.
--
-- > openVCache   :: Int -> FilePath -> IO VCache
-- > vcacheSubdir :: ByteString -> VCache -> VCache
-- > loadRootPVar :: (VCacheable a) => VCache -> ByteString -> a -> PVar a
--
-- The normal use of VCache is to open VCache in the main function, 
-- use vcacheSubdir for each major framework, plugin, or independent
-- component that might need persistent storage, then load at most a
-- few root PVars per component. Most domain modeling should be at 
-- the data layer, i.e. the type held by the PVar.
--
-- See VSpace, VRef, and PVar for more information.
data VCache = VCache
    { vcache_space :: !VSpace -- ^ virtual address space for VCache
    , vcache_path  :: !ByteString
    } deriving (Eq)

-- | VSpace is the virtual address space used by VCache. VSpace allows
-- construction of VRefs and anonymous PVars, and is necessary to run
-- transactions. VSpace may be acquired via VRef, PVar, or VCache. 
--
-- This virtual address space can be much larger than system memory,
-- potentially many terabytes. The space is backed by file, enabling
-- easy persistence of data structures. The space is elastic: a PVar
-- can easily vary in size from one update to another.
--
data VSpace = VSpace
    { vcache_lockfile   :: !FileLock -- block concurrent use of VCache file

    -- LMDB contents. 
    , vcache_db_env     :: !MDB_env
    , vcache_db_memory  :: {-# UNPACK #-} !MDB_dbi' -- address → value
    , vcache_db_vroots  :: {-# UNPACK #-} !MDB_dbi' -- path → address 
    , vcache_db_caddrs  :: {-# UNPACK #-} !MDB_dbi' -- hashval → [address]
    , vcache_db_refcts  :: {-# UNPACK #-} !MDB_dbi' -- address → Word64
    , vcache_db_refct0  :: {-# UNPACK #-} !MDB_dbi' -- address → ()

    , vcache_allocator  :: !(IORef Allocator) -- allocate new VRefs and PVars
    , vcache_signal     :: !(MVar ()) -- signal writer that work is available
    , vcache_writes     :: !(TVar Writes) -- STM layer PVar writes
    , vcache_rwlock     :: !RWLock -- replace gap left by MDB_NOLOCK


    , vcache_mem_vrefs  :: !(IORef EphMap) -- track VRefs in memory
    , vcache_mem_pvars  :: !(IORef PVEphMap) -- track PVars in memory

    , vcache_c_limit    :: !(IORef Int) -- weight limit
    , vcache_c_size     :: !(IORef Int) -- estimated current size (for stats)

    -- Signal writes mostly exists to prevent GC of PVars until after 
    -- any updated PVars are durable. I also use it to maintain stats. :)
    , vcache_signal_writes :: !(Writes -> IO ()) -- signal durable writes
    , vcache_ct_writes  :: !(IORef WriteCt) -- (stat) information about writes

    , vcache_alloc_init :: {-# UNPACK #-} !Address -- (for stats) initial allocator on open


    -- Still needed:
    --  data for GC guidance
    --  data for cache guidance

    -- share persistent variables for safe STM

    -- Further, I need...
    --   log or queue of 'new' vrefs and pvars, 
    --     including those still being written
    --   a thread performing writes and incremental GC
    --   a channel to talk to that thread
    --   queue of MVars waiting on synchronization/flush.

    }

instance Eq VSpace where (==) = (==) `on` vcache_signal

-- needed: a transactional queue of updates to PVars

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
-- N-2 readers. This assumes we will hold RdOnlyLock whenever we
-- try to construct a VRef. (Note: I would need to hold onto the
-- content for an additional step if I'm going to 'free' content
-- explicitly. But, for now, I'll leave it to GC via ByteString.)
--
-- The allocator will double as a log of recently allocated data.
-- Mostly, this is important when allocating new PVars, or when
-- loading a recently allocated VRef.
--
-- In addition to VRefs, I need to track allocated root PVars to
-- prevent a given root from being assigned two addresses. The 
-- new anonymous PVar may be updated more conventionally, but 
-- will be written at least once after allocation
--
-- Allocated values must become persistent before any update on
-- PVars that will include references to these values. Fortunately,
-- this should not be difficult to achieve, and can mostly run in
-- a background thread.
--
-- Note: allocations are processed separately from PVar updates.
-- A goal here is to take advantage of the fast MDB_APPEND mode.
--
data Allocator = Allocator
    { alloc_new_addr :: {-# UNPACK #-} !Address -- next address
    , alloc_frm_next :: !AllocFrame -- frame N+1 (next step)
    , alloc_frm_curr :: !AllocFrame -- frame N   (curr step)
    , alloc_frm_prev :: !AllocFrame -- frame N-1 (prev step)
    }

data AllocFrame = AllocFrame 
    { alloc_list :: !(Map Address Allocation)       -- allocated addresses
    , alloc_seek :: !(Map ByteString [Allocation])  -- named addresses
    , alloc_init :: {-# UNPACK #-} !Address         -- alloc_new_addr at frame init.
    }

data Allocation = Allocation
    { alloc_name :: {-# UNPACK #-} !ByteString -- VRef hash or PVar path, or empty for anon PVar
    , alloc_data :: {-# UNPACK #-} !ByteString -- initial content
    , alloc_addr :: {-# UNPACK #-} !Address    -- where to save content
    , alloc_deps :: [PutChild]                 -- keepalive for allocation
    }

allocFrameSearch :: (AllocFrame -> Maybe a) -> Allocator -> Maybe a
allocFrameSearch f a = f n <|> f c <|> f p where
    n = alloc_frm_next a
    c = alloc_frm_curr a
    p = alloc_frm_prev a

-- thoughts: I may need an additional allocations list for anonymous
-- PVars, if only to support a `newPVarIO` or similar.

-- simple read-only operations 
--  LMDB transaction is aborted when finished, so cannot open DBIs
withRdOnlyTxn :: VSpace -> (MDB_txn -> IO a) -> IO a
withRdOnlyTxn vc = withLock . bracket newTX endTX where
    withLock = withRdOnlyLock (vcache_rwlock vc)
    newTX = mdb_txn_begin (vcache_db_env vc) Nothing True
    endTX = mdb_txn_abort
{-# INLINE withRdOnlyTxn #-}

withByteStringVal :: ByteString -> (MDB_val -> IO a) -> IO a
withByteStringVal (BSI.PS fp off len) action = withForeignPtr fp $ \ p ->
    action $ MDB_val { mv_size = fromIntegral len
                     , mv_data = p `plusPtr` off }
{-# INLINE withByteStringVal #-}

-- | The VTx transactions allow atomic updates to both PVars from 
-- one VSpace and arbitrary STM resources (TVars, TArrays, and so
-- on). The ability to lift STM operations provides a convenient
-- basis for composition of persistent and ephemeral data models.
--
-- VTx transactions support the full set of ACID properties, but are
-- only ACI by default. Durability is optional. Durability may be 
-- indicated at any time during the transaction, depending on the 
-- domain model and states involved. Non-durable transactions have
-- just a small amount of additional overhead compared to STM. But
-- durability requires an expensive wait for a background writer
-- thread to signal that content is fully consistent with the disk.
--
-- Transactions that run around the same time will frequently be
-- batched, i.e. such that the writer only serializes occasional
-- intermediate values for rapidly updating PVars. Synchronization
-- overheads will often be amortized among many transactions.
-- 
-- Note: Long-running VCache transactions can starve, restarting
-- on a regular basis. Favor short transactions and try to delay
-- expensive computations until after the transaction commits.
-- 
newtype VTx a = VTx { _vtx :: StateT VTxState STM a }
    deriving (Monad, Functor, Applicative, Alternative, MonadPlus)

-- | In addition to the STM transaction, I need to track whether
-- the transaction is durable (such that developers may choose 
-- based on internal domain-model concerns) and which variables
-- have been read or written. All PVars involved must be part of
-- the same VSpace.
data VTxState = VTxState
    { vtx_space     :: !VSpace
    , vtx_writes    :: !WriteLog
    , vtx_durable   :: !Bool
    }

-- | run an arbitrary STM operation as part of a VTx transaction.
liftSTM :: STM a -> VTx a
liftSTM = VTx . lift
{-# INLINE liftSTM #-}

getVTxSpace :: VTx VSpace
getVTxSpace = VTx (gets vtx_space)
{-# INLINE getVTxSpace #-}

-- | add a PVar write to the VTxState.
-- (Does not modify the underlying TVar.)
markForWrite :: PVar a -> a -> VTx ()
markForWrite pv a = VTx $ modify $ \ vtx ->
    let txw = TxW pv a in
    let addr = pvar_addr pv in
    let writes' = Map.insert addr txw (vtx_writes vtx) in
    vtx { vtx_writes = writes' }
{-# INLINE markForWrite #-}







type WriteLog  = Map Address TxW
data TxW = forall a . TxW !(PVar a) a
    -- Note: I can either record just the PVar, or the PVar and its value.
    -- The latter is favorable because it avoids risk of creating very large
    -- transactions in the writer thread (i.e. to read the updated PVars),
    -- and thus reduces risk of starvation.

data Writes = Writes 
    { write_data :: !WriteLog
    , write_sync :: ![MVar ()]
    }
data WriteCt = WriteCt
    { wct_frames :: {-# UNPACK #-} !Int -- how many write frames
    , wct_pvars  :: {-# UNPACK #-} !Int -- how many PVars written
    , wct_sync   :: {-# UNPACK #-} !Int -- how many sync requests
    }




-- So for now, I'm keeping the value written redundantly in the
-- TxW so that I don't need to read the PVars in the writer thread.
--
-- I'm also holding onto the PVars, not just the addresses, because
-- it is useful to guarantee against garbage collection.

data VTxBatch = VTxBatch SyncOp WriteLog 
type SyncOp = IO () -- called after write is synchronized.

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

putChildAddr :: PutChild -> Address
putChildAddr (PutChild (Left (PVar { pvar_addr = x }))) = x
putChildAddr (PutChild (Right (VRef { vref_addr = x }))) = x


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


