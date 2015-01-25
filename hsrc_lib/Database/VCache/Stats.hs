
module Database.VCache.Stats
    ( VCacheStats(..)
    , vcacheStats
    ) where

import Database.LMDB.Raw
import Database.VCache.Types
import Data.IORef
import qualified Data.IntMap as IntMap

-- | Compute some miscellaneous statistics for a VCache instance at
-- runtime. These aren't really useful for anything, except to gain
-- some confidence about activity or comprehension of performance. 
vcacheStats :: VCache -> IO VCacheStats
vcacheStats (VCache vc _) = withRdOnlyTxn vc $ \ txnStat -> do
    let db = vcache_db_env vc
    envInfo <- mdb_env_info db
    envStat <- mdb_env_stat db
    memStat <- mdb_stat' txnStat (vcache_db_memory vc)
    rootStat <- mdb_stat' txnStat (vcache_db_vroots vc)
    hashStat <- mdb_stat' txnStat (vcache_db_caddrs vc)
    memVRefsMap <- readIORef (vcache_mem_vrefs vc)
    memPVarsMap <- readIORef (vcache_mem_pvars vc)
    allocSt <- readIORef (vcache_allocator vc)
    let fileSize = (fromIntegral $ me_last_pgno envInfo) * (fromIntegral $ ms_psize envStat)
    let vrefCount = (fromIntegral $ ms_entries hashStat) 
    let pvarCount = (fromIntegral $ ms_entries memStat) - vrefCount
    let rootCount = (fromIntegral $ ms_entries rootStat)
    let memVRefs = IntMap.size memVRefsMap
    let memPVars = IntMap.size memPVarsMap
    let allocPos = alloc_new_addr allocSt
    return $ VCacheStats
        { vcstat_file_size = fileSize
        , vcstat_vref_count = vrefCount
        , vcstat_pvar_count = pvarCount
        , vcstat_root_count = rootCount
        , vcstat_mem_vref = memVRefs
        , vcstat_mem_pvar = memPVars
        , vcstat_alloc_pos = allocPos
        }

