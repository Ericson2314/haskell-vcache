{-# LANGUAGE DeriveDataTypeable, ExistentialQuantification, GeneralizedNewtypeDeriving #-}
{-# LANGUAGE BangPatterns #-}

-- Internal file. Lots of types. Lots of coupling.
module Database.VCache.Types
    ( Address, isVRefAddr, isPVarAddr
    , VRef(..), Cache(..), CacheMode(..)
    , VREph(..), VREphMap, addVREph, takeVREph
    , PVar(..), RDV(..)
    , PVEph(..), PVEphMap, addPVEph
    , VCache(..), VSpace(..)
    , VPut(..), VPutS(..), VPutR(..)
    , VGet(..), VGetS(..), VGetR(..)
    , VCacheable(..)
    , Allocator(..), AllocFrame(..)
    , GC(..), GCFrame(..)
    , Memory(..)
    , VTx(..), VTxState(..), TxW(..), VTxBatch(..)
    , Writes(..), WriteLog, WriteCt(..)
    , CacheSizeEst(..)

    -- misc. utilities
    , allocFrameSearch
    , recentGC

    , withRdOnlyTxn
    , withByteStringVal

    , getVTxSpace, markForWrite, liftSTM
    , mkVRefCache, cacheModeBits, touchCache
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

-- | A VRef is an opaque reference to an immutable value backed by a
-- file, specifically via LMDB. The primary motivation for VRefs is
-- to support memory-cached values, i.e. very large data structures
-- that should not be stored in all at once in RAM.
--
-- The API involving VRefs is conceptually pure.
--
-- > vref  :: (VCacheable a) => VSpace -> a -> VRef a
-- > deref :: VRef a -> a
--
-- Under the hood, each VRef has a 64-bit address and a local cache.
-- When dereferenced, the cache is checked or the value is read from
-- the database then cached. Variants of vref and deref control cache
-- behavior.
-- 
-- VCacheable values may themselves contain VRefs and PVars, storing
-- just the address. Very large structured data is readily modeled
-- by using VRefs to load just the pieces you need. However, there is
-- one major constraint:
--
-- VRefs may only represent acyclic structures. 
--
-- If developers want cyclic structure, they need a PVar in the chain.
-- Alternatively, cycles may be modeled indirectly using explicit IDs.
-- 
-- Besides memory caching, VRefs also utilize structure sharing: all
-- VRefs sharing the same serialized representation will share the 
-- same address. Structure sharing enables VRefs to be compared for
-- equality without violating conceptual purity. It also simplifies
-- reasoning about idempotence, storage costs, memoization, etc..
--
data VRef a = VRef 
    { vref_addr   :: {-# UNPACK #-} !Address            -- ^ address within the cache
    , vref_cache  :: {-# UNPACK #-} !(IORef (Cache a))  -- ^ cached value & weak refs 
    , vref_space  :: !VSpace                            -- ^ virtual address space for VRef
    , vref_type   :: !TypeRep                           -- ^ type of value held by VRef
    , vref_parse  :: !(VGet a)                          -- ^ parser for this VRef
    } deriving (Typeable)
instance Eq (VRef a) where (==) = (==) `on` vref_cache
instance Show (VRef a) where 
    showsPrec _ v = showString "VRef#" . shows (vref_addr v)
                  . showString "::" . shows (vref_type v)

-- For every VRef we have in memory, we need an ephemeron in a table.
-- This ephemeron table supports structure sharing, caching, and GC.
-- I model this ephemeron by use of `mkWeakMVar`.
data VREph = forall a . VREph 
    { vreph_addr  :: {-# UNPACK #-} !Address
    , vreph_type  :: !TypeRep
    , vreph_cache :: {-# UNPACK #-} !(Weak (IORef (Cache a)))
    } 
type VREphMap = Map Address (Map TypeRep VREph)
    -- Address is at the top layer of the map mostly to simplify GC.


addVREph :: VREph -> VREphMap -> VREphMap
addVREph e = Map.alter (Just . maybe i0 ins) (vreph_addr e) where
    ty = vreph_type e
    i0 = Map.singleton ty e
    ins = Map.insert ty e
{-# INLINABLE addVREph #-}
                    
takeVREph :: Address -> TypeRep -> VREphMap -> Maybe (VREph, VREphMap)
takeVREph !addr !ty !em = 
    case Map.lookup addr em of
         Nothing -> Nothing
         Just tym -> case Map.lookup ty tym of
            Nothing -> Nothing
            Just e -> 
                let em' = if (1 == Map.size tym)
                        then Map.delete addr em 
                        else Map.insert addr (Map.delete ty tym) em
                in
                Just (e, em') 
{-# INLINABLE takeVREph #-}


-- TODO: I may need a way to "reserve" VRef addresses for destruction, 
-- i.e. such that I can guard against 

-- Every VRef contains its own cache. Thus, there is no extra lookup
-- overhead to test the cache. The cache includes an integer as a
-- bitfield to describe mode.
data Cache a 
        = NotCached 
        | Cached a {-# UNPACK #-} !Word16

--
-- cache bitfield for mode:
--   bit 0..4: weight, log scale
--     log scale: 2^(N+6), max N=31
--   bits 5..6: cache mode 0..3
--   bit 7: toggle; set 1 by manager, 0 by derefc
--
-- Weight is used to guide aggressiveness of the cache manager. 
-- Currently, it just records the size of the encoded value, and
-- does not account for expansion of values after parse. 


-- | Cache modes are used when deciding, heuristically, whether to
-- clear a value from cache. These modes don't have precise meaning,
-- but there is a general intention: higher numbered modes indicate
-- that VCache should hold onto a value for longer or with greater
-- priority. In the current implementation, CacheMode is used as a
-- pool of 'hitpoints' in a gaming metaphor: if an entry would be
-- removed, but its mode is greater than zero, the mode is reduced
-- instead.
--
-- The default for vref and deref is CacheMode1. Use of vrefc or 
-- derefc may specify other modes. Cache mode is monotonic: if
-- the same VRef is deref'd with two different modes, the higher
-- mode will be favored.
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


-- | clear bit 7; adjust cache mode monotonically.
touchCache :: CacheMode -> Word16 -> Word16
touchCache !cm !w =
    let cb' = (w .&. 0x60) `max` cacheModeBits cm in
    (w .&. 0xff1f) .|. cb'
{-# INLINE touchCache #-}

mkVRefCache :: a -> Int -> CacheMode -> Cache a
mkVRefCache val !w !cm = Cached val cw where
    cw = m .|. cs 0 64
    cs r k = if ((k > w) || (r == 0x1f)) then r else cs (r+1) (k*2)
    m = cacheModeBits cm

-- | A PVar is a mutable variable backed by VCache. PVars can be read
-- or updated transactionally (see VTx), and may store by reference
-- as part of domain data (see VCacheable). 
--
-- A PVar is not cached. If you want memory cached contents, you'll 
-- need a PVar that contains one or more VRefs. However, the first 
-- read from a PVar is lazy, so merely referencing a PVar does not 
-- require loading its contents into memory.
--
-- Due to how updates are batched, high frequency or bursty updates 
-- on a PVar should perform acceptably. Not every intermediate value 
-- is written to disk.
--
-- Anonymous PVars will be garbage collected if not in use. Persistence
-- requires ultimately tying contents to named roots (cf. loadRootPVar).
-- Garbage collection is based on reference counting, so developers must
-- be cautious when working with cyclic data, i.e. break cycles before
-- disconnecting them from root.
--
-- Note: PVars must never contain undefined or error values, nor any
-- value that cannot be serialized by a VCacheable instance. 
--
data PVar a = PVar
    { pvar_addr  :: {-# UNPACK #-} !Address
    , pvar_data  :: {-# UNPACK #-} !(TVar (RDV a))
    , pvar_space :: !VSpace -- ^ virtual address space for PVar
    , pvar_type  :: !TypeRep
    , pvar_write :: !(Writer a)
    } deriving (Typeable)

type Writer a = (a -> VPut ())
instance Eq (PVar a) where (==) = (==) `on` pvar_data
instance Show (PVar a) where 
    showsPrec _ pv = showString "PVar#" . shows (pvar_addr pv)
                   . showString "::" . shows (pvar_type pv)

-- ephemeron table for PVars.
data PVEph = forall a . PVEph 
    { pveph_addr :: {-# UNPACK #-} !Address
    , pveph_type :: !TypeRep
    , pveph_data :: {-# UNPACK #-} !(Weak (TVar (RDV a)))
    }
type PVEphMap = Map Address PVEph

addPVEph :: PVEph -> PVEphMap -> PVEphMap
addPVEph pve = Map.insert (pveph_addr pve) pve
{-# INLINE addPVEph #-}

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

-- | VSpace represents the virtual address space used by VCache. Except
-- for loadRootPVar, most operations use VSpace rather than the VCache.
-- VSpace is accessed by vcache_space, vref_space, or pvar_space.
--
-- Addresses from this space are allocated incrementally, odds to PVars
-- and evens to VRefs, independent of object size. The space is elastic:
-- it isn't a problem to change the size of PVars (even drastically) from
-- one update to another.
--
-- In theory, VSpace could run out of 64-bit addresses. In practice, this
-- isn't a real concern - a quarter million years at a sustained million 
-- allocations per second. 
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

    , vcache_memory     :: !(MVar Memory) -- Haskell-layer memory management
    , vcache_signal     :: !(MVar ()) -- signal writer that work is available
    , vcache_writes     :: !(TVar Writes) -- STM-layer PVar writes
    , vcache_rwlock     :: !RWLock -- replace gap left by MDB_NOLOCK

    , vcache_alloc_init :: {-# UNPACK #-} !Address -- (for stats) initial allocator on open

    , vcache_gc_start   :: !(IORef (Maybe Address)) -- supports incremental GC
    , vcache_gc_count   :: !(IORef Int) -- (stat) number of addresses GC'd

    , vcache_climit     :: !(IORef Int) -- targeted max cache size in bytes
    , vcache_csize      :: !(IORef CacheSizeEst) -- estimated cache sizes
    , vcache_cvrefs     :: !(MVar VREphMap) -- track just the cached VRefs


    -- share persistent variables for safe STM

    -- Further, I need...
    --   log or queue of 'new' vrefs and pvars, 
    --     including those still being written
    --   a thread performing writes and incremental GC
    --   a channel to talk to that thread
    --   queue of MVars waiting on synchronization/flush.

    } deriving (Typeable)

instance Eq VSpace where (==) = (==) `on` vcache_signal

-- needed: a transactional queue of updates to PVars

-- | The Allocator both tracks the 'bump-pointer' address for the
-- next allocation, plus in-memory logs for recent and near future 
-- allocations.
--
-- The log has three frames, based on the following observations:
--
-- * frames are rotated when the writer lock is held
-- * when the writer lock is held, readers exist for two prior frames
-- * readers from two frames earlier use log to find allocations from:
--   * the previous write frame
--   * the current write frame
--   * the next write frame (allocated during write)
-- 
-- Each write frame includes content for both the primary (db_memory)
-- and secondary (db_caddrs or db_vroots) indices. 
--
-- Normal Data.Map is favored because I want the keys in sorted order
-- when writing into the LMDB layer anyway.
--
data Allocator = Allocator
    { alloc_new_addr :: {-# UNPACK #-} !Address -- next address
    , alloc_frm_next :: !AllocFrame -- frame N+1 (next step)
    , alloc_frm_curr :: !AllocFrame -- frame N   (curr step)
    , alloc_frm_prev :: !AllocFrame -- frame N-1 (prev step)
    }

-- a single allocation frame corresponds to new content written
-- while the writer is busy in the background. 
data AllocFrame = AllocFrame 
    { alloc_list :: !(Map Address ByteString)    -- recent allocations
    , alloc_seek :: !(Map ByteString [Address])  -- vref content addressing 
    , alloc_root :: ![(Address, ByteString)]     -- root PVars with path names
    , alloc_init :: {-# UNPACK #-} !Address      -- next address at frame start.
    }

allocFrameSearch :: (AllocFrame -> Maybe a) -> Allocator -> Maybe a
allocFrameSearch f a = f n <|> f c <|> f p where
    n = alloc_frm_next a
    c = alloc_frm_curr a
    p = alloc_frm_prev a

-- | In addition to recent allocations, we track garbage collection.
-- The goal here is to prevent revival of VRefs after we decide to
-- delete them. So, when we try to allocate a VRef, we'll check to
-- see whether its address has been targeted for deletion.
--
-- To keep this simple, GC is performed by the writer thread. Other
-- threads must worry about reading outdated reference counts. This
-- also means we only need the two frames: a reader of frame N-2  
-- only needs to prevent revival of VRefs GC'd at N-1 or N.
--
-- ASIDE: a nursery GC pass eliminates transient VRefs that needn't
-- be recorded in the database, e.g. for a short-lived trie node. In
-- this case, we may simply remove the reference from the allocator.
--
data GC = GC 
    { gc_frm_curr :: !GCFrame
    , gc_frm_prev :: !GCFrame
    } 
data GCFrame = forall a . GCFrame !(Map Address a)
    -- The concrete map type depends on the writer

recentGC :: GC -> Address -> Bool
recentGC gc addr = ff c || ff p where
    ff (GCFrame m) = Map.member addr m
    c = gc_frm_curr gc
    p = gc_frm_prev gc
{-# INLINE recentGC #-}

-- | The Memory datatype tracks allocations, GC, and ephemeron
-- tables for tracking both PVars and VRefs in Haskell memory.
-- These are combined into one type mostly because typical 
-- operations on them are atomic... and STM isn't permitted 
-- because vref constructors are used with unsafePerformIO.
data Memory = Memory
    { mem_vrefs  :: !VREphMap   -- ^ In-memory VRefs
    , mem_pvars  :: !PVEphMap   -- ^ In-memory PVars
    , mem_gc     :: !GC         -- ^ recently GC'd addresses (two frames)
    , mem_alloc  :: !Allocator  -- ^ recent or pending allocations (three frames)
    }


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
    action $ MDB_val { mv_size = fromIntegral len, mv_data = p `plusPtr` off }
{-# INLINE withByteStringVal #-}

-- | The VTx transactions allow developers to atomically manipulate
-- PVars and STM resources (TVars, TArrays, etc..). VTx is a thin
-- layer above STM, additionally tracking which PVars are written so
-- it can push the batch to a background writer thread upon commit.
-- 
-- VTx supports full ACID semantics (atomic, consistent, isolated,
-- durable), but durability is optional (see markDurable). 
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

-- | add a PVar write to the VTxState. This should be combined with
-- a function that modifies the underlying TVar.
markForWrite :: PVar a -> a -> VTx ()
markForWrite pv a = VTx $ modify $ \ vtx ->
    let txw = TxW (pvar_write pv) a in
    let addr = pvar_addr pv in
    let writes' = Map.insert addr txw (vtx_writes vtx) in
    vtx { vtx_writes = writes' }
{-# INLINE markForWrite #-}


-- | Estimate for cache size is based on random samples with an
-- exponential moving average. It isn't very precise, but it is
-- good enough for the purpose of guiding aggressiveness of the
-- exponential decay model.
data CacheSizeEst = CacheSizeEst
    { csze_addr_size  :: {-# UNPACK #-} !Double -- average of sizes
    , csze_addr_sqsz  :: {-# UNPACK #-} !Double -- average of squares
    }

type WriteLog  = Map Address TxW 
data TxW = forall a . TxW !(Writer a) a
    -- Note: I can either record just the PVar, or the PVar and its value.
    -- The latter is favorable because it avoids risk of creating very large
    -- transactions in the writer thread (i.e. to read the updated PVars).

-- | Collection of writes 
data Writes = Writes 
    { write_data :: !WriteLog
    , write_sync :: ![MVar ()]
    }
    -- Design Thoughts: It might be worthwhile to separate the writelog
    -- and synchronization, i.e. to potentially reduce conflicts between
    -- transactions. But I'll leave this to later.

data WriteCt = WriteCt
    { wct_frames :: {-# UNPACK #-} !Int -- how many write frames
    , wct_pvars  :: {-# UNPACK #-} !Int -- how many PVars written
    , wct_sync   :: {-# UNPACK #-} !Int -- how many sync requests
    }

data VTxBatch = VTxBatch SyncOp WriteLog 
type SyncOp = IO () -- called after write is synchronized.

type Ptr8 = Ptr Word8
type PtrIni = Ptr8
type PtrEnd = Ptr8
type PtrLoc = Ptr8

-- | VPut is a serialization monad akin to Data.Binary or Data.Cereal.
-- However, VPut is not restricted to pure binaries: developers may
-- include VRefs and PVars in the output.
--
-- Content emitted by VPut will generally be read only by VCache. So
-- it may be worth optimizing some cases, such as lists are written 
-- in reverse such that readers won't need to reverse the list.
newtype VPut a = VPut { _vput :: VPutS -> IO (VPutR a) }
data VPutS = VPutS 
    { vput_space    :: !VSpace                -- ^ destination space
    , vput_children :: ![Address]             -- ^ addresses written (addends buffer)
    , vput_buffer   :: !(IORef PtrIni)        -- ^ buffer for easy free, realloc
    , vput_target   :: {-# UNPACK #-} !PtrLoc -- ^ location within buffer
    , vput_limit    :: {-# UNPACK #-} !PtrEnd -- ^ current limit for input
    }


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


-- | VGet is a parser combinator monad for VCache. Unlike pure binary
-- parsers, VGet supports reads from a stack of VRefs and PVars to 
-- directly model structured data.
--
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
-- Developers must ensure that `get` on the serialization from `put` 
-- returns the same value. And `get` should be backwards compatible
-- to older versions of the same type. Consider version wrappers, 
-- cf. SafeCopy package.
-- 
class (Typeable a) => VCacheable a where 
    -- | Serialize a value as a stream of bytes and value references. 
    put :: a -> VPut ()

    -- | Parse a value from its serialized representation into memory.
    get :: VGet a

