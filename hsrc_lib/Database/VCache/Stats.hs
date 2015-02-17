
module Database.VCache.Stats
    ( VCacheStats(..)
    , vcacheStats
    ) where

import Data.IORef
import Control.Concurrent.MVar
import qualified Data.Map.Strict as Map

import Database.LMDB.Raw
import Database.VCache.Types


-- | Miscellaneous statistics for a VCache instance. These are not
-- necessarily consistent, current, or useful. But they can say a
-- a bit about the liveliness and health of a VCache system.
data VCacheStats = VCacheStats
        { vcstat_file_size      :: {-# UNPACK #-} !Int  -- ^ estimated database file size (in bytes)
        , vcstat_vref_count     :: {-# UNPACK #-} !Int  -- ^ number of immutable values in the database
        , vcstat_pvar_count     :: {-# UNPACK #-} !Int  -- ^ number of mutable PVars in the database
        , vcstat_root_count     :: {-# UNPACK #-} !Int  -- ^ number of named roots (a subset of PVars)
        , vcstat_mem_vrefs      :: {-# UNPACK #-} !Int  -- ^ number of VRefs in Haskell process memory
        , vcstat_mem_pvars      :: {-# UNPACK #-} !Int  -- ^ number of PVars in Haskell process memory
        , vcstat_eph_count      :: {-# UNPACK #-} !Int  -- ^ number of addresses with zero references
        , vcstat_alloc_pos      :: {-# UNPACK #-} !Address -- ^ address to next be used by allocator
        , vcstat_alloc_count    :: {-# UNPACK #-} !Int  -- ^ number of allocations by this process 
        , vcstat_cache_limit    :: {-# UNPACK #-} !Int  -- ^ target cache size in bytes 
        , vcstat_cache_size     :: {-# UNPACK #-} !Int  -- ^ estimated cache size in bytes
        , vcstat_gc_count       :: {-# UNPACK #-} !Int  -- ^ number of addresses GC'd by this process
        , vcstat_write_pvars    :: {-# UNPACK #-} !Int  -- ^ number of PVar updates to disk (after batching)
        , vcstat_write_sync     :: {-# UNPACK #-} !Int  -- ^ number of sync requests (~ durable transactions)
        , vcstat_write_frames   :: {-# UNPACK #-} !Int  -- ^ number of LMDB-layer transactions by this process
        } deriving (Show, Ord, Eq)

-- | Compute some miscellaneous statistics for a VCache instance at
-- runtime. These aren't really useful for anything, except to gain
-- some confidence about activity or comprehension of performance. 
vcacheStats :: VSpace -> IO VCacheStats
vcacheStats vc = withRdOnlyTxn vc $ \ txnStat -> do
    let db = vcache_db_env vc
    envInfo <- mdb_env_info db
    envStat <- mdb_env_stat db
    dbMemStat <- mdb_stat' txnStat (vcache_db_memory vc)
    rootStat <- mdb_stat' txnStat (vcache_db_vroots vc)
    hashStat <- mdb_stat' txnStat (vcache_db_caddrs vc)
    ephStat <- mdb_stat' txnStat (vcache_db_refct0 vc)
    memory <- readMVar (vcache_memory vc)
    gcCount <- readIORef (vcache_gc_count vc)
    wct <- readIORef (vcache_ct_writes vc)
    cLimit <- readIORef (vcache_climit vc)
    cSizeEst <- readIORef (vcache_csize vc)
    cvrefs <- readMVar (vcache_cvrefs vc)
    
    let fileSize = (1 + (fromIntegral $ me_last_pgno envInfo)) 
                 * (fromIntegral $ ms_psize envStat)
    let vrefCount = (fromIntegral $ ms_entries hashStat) 
    let pvarCount = (fromIntegral $ ms_entries dbMemStat) - vrefCount
    let ephCount = (fromIntegral $ ms_entries ephStat)
    let rootCount = (fromIntegral $ ms_entries rootStat)
    let cacheSizeBytes = ceiling $ fromIntegral (Map.size cvrefs)
                                 * sqrt (csze_addr_sqsz cSizeEst)
    let memVRefsCount = Map.foldl' (\ a b -> a + Map.size b) 0 (mem_vrefs memory)
    let memPVarsCount = Map.size (mem_pvars memory)
    let allocPos = alloc_new_addr (mem_alloc memory)
    let allocDiff = allocPos - vcache_alloc_init vc
    let allocCount = fromIntegral $ allocDiff `div` 2 
    return $ VCacheStats
        { vcstat_file_size = fileSize
        , vcstat_vref_count = vrefCount
        , vcstat_pvar_count = pvarCount
        , vcstat_root_count = rootCount
        , vcstat_mem_vrefs = memVRefsCount
        , vcstat_mem_pvars = memPVarsCount
        , vcstat_eph_count = ephCount
        , vcstat_alloc_pos = allocPos
        , vcstat_alloc_count = allocCount
        , vcstat_cache_limit = cLimit
        , vcstat_cache_size = cacheSizeBytes
        , vcstat_write_sync = wct_sync wct
        , vcstat_write_pvars = wct_pvars wct
        , vcstat_write_frames = wct_frames wct
        , vcstat_gc_count = gcCount
        }

