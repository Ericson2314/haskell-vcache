-- Implementation of the Writer threads.
--
-- Some general design goals here:
--
--   Favor sequential processing (not random access)
--   Single read/write pass per database per frame
--   Append newly written content when possible
--
-- Currently these goals are mostly met, except that I perform two
-- passes through vcache_db_refct0 - once to select addresses for
-- GC, and later to update reference counts.
--
-- My earlier design didn't account for `newPVar` allowing addresses
-- to be undefined pending completion of VTx transactions. I'll need
-- to adjust for this soon.
--   
module Database.VCache.Write
    ( writerStep
    ) where

import Control.Monad
import Control.Exception
import Control.Concurrent
import Control.Concurrent.STM
import Data.IORef
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.List as L
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Foreign.Ptr
import Foreign.Storable
import Foreign.Marshal.Alloc

import Database.LMDB.Raw
import Database.VCache.Types 
import Database.VCache.VPutFini -- serialize updated PVars
import Database.VCache.VGetInit -- read dependencies to manage refcts
import Database.VCache.RWLock -- need a writer lock
import Database.VCache.Refct  -- to update reference counts

-- when processing write batches, we'll need to track
-- differences in reference counts for each address.
type RefctDiff = Map Address Refct

addrSize :: Int
addrSize = sizeOf (undefined :: Address)

-- Single step for VCache writer.
writerStep :: VSpace -> IO ()
writerStep vc = withRWLock (vcache_rwlock vc) $ do
    takeMVar (vcache_signal vc) 
    ws <- atomically (takeWrites (vcache_writes vc))
    wb <- seralizeWrites (write_data ws)
    af <- allocatorFrameStep vc
    let ab = Map.map fnWriteAlloc (alloc_list af) -- Map Address ByteString
    let ub = Map.union wb ab -- favor written data over allocations
    let allocInit = alloc_init af 
    txn <- mdb_txn_begin (vcache_db_env vc) Nothing False

    -- Select a batch of addresses for GC.
    let gcLimit = max 1000 (2 * Map.size ub) -- for incremental GC
    gcb <- runGarbageCollector vc txn gcLimit
    let fb = Map.union gcb ub   -- gcb and ub should be independent
    let bSep = Map.size fb == (Map.size ub + Map.size gcb) 
    unless bSep (fail "VCache bug: GC and update on same address")

    -- update the virtual memory, and obtain information about changes
    rcDiff <- updateVirtualMemory vc txn allocInit fb

    -- update reference counts.
    let rcAlloc = fmap (\ an -> if isNewRoot an then 1 else 0) (alloc_list af)  -- new references from allocations
    let rcBatch = Map.unionWith (+) rcDiff rcAlloc                              -- plus references for allocations
    updateReferenceCounts vc txn allocInit rcBatch                              -- write batch of reference counts

    -- record secondary indexes: PVar roots, VRef hashes
    -- TODO: include *deleted* VRefs, i.e. a map of Hashes to Addresses.
    --   I'll probably need to remap allocations, in this case.
    writeSecondaryIndexes vc txn (alloc_seek af)

    -- finish up
    mdb_txn_commit txn
    modifyIORef' (vcache_gc_count vc) (+ (Map.size gcb)) -- update GC count
    vcache_signal_writes vc ws  -- report writes & prevent early GC of writes
    mapM_ syncSignal (write_sync ws) -- signal durable transaction threads
{-# NOINLINE writerStep #-}

allocatorFrameStep :: VSpace -> IO AllocFrame
allocatorFrameStep vc = modifyMVarMasked (vcache_memory vc) (return . allocFrameStep)
{-# INLINE allocatorFrameStep #-}

-- rotate frames and return the new 'current' frame
allocFrameStep :: Memory -> (Memory, AllocFrame)
allocFrameStep mem = (mem', alloc_frm_next ac) where
    ac = mem_alloc mem
    addr = alloc_new_addr ac
    ac' = Allocator
        { alloc_new_addr = addr
        , alloc_frm_next = newAllocFrame addr
        , alloc_frm_curr = alloc_frm_next ac
        , alloc_frm_prev = alloc_frm_curr ac
        }
    mem' = mem { mem_alloc = ac' }

newAllocFrame :: Address -> AllocFrame
newAllocFrame addr = AllocFrame
    { alloc_init = addr
    , alloc_seek = Map.empty
    , alloc_list = Map.empty
    }
{-# INLINABLE newAllocFrame #-}

    
    


isNewRoot :: Allocation -> Bool
isNewRoot an = isPVarAddr (alloc_addr an) && not (BS.null (alloc_name an))

takeWrites :: TVar Writes -> STM Writes
takeWrites tv = do
    wb <- readTVar tv
    writeTVar tv (Writes Map.empty [])
    return wb

seralizeWrites :: WriteLog -> IO WriteBatch
seralizeWrites = Map.traverseWithKey (const writeTxW)

writeTxW :: TxW -> IO WriteCell
writeTxW (TxW pv v) =
    runVPutIO (pvar_space pv) (pvar_write pv v) >>= \ ((), _data, _deps) ->
    return (_data,_deps)

fnWriteAlloc :: Allocation -> WriteCell
fnWriteAlloc an = (alloc_data an, alloc_deps an)

syncSignal :: MVar () -> IO ()
syncSignal mv = void (tryPutMVar mv ())

-- Write the PVar roots and VRef hashmap. In these cases, the address
-- is the data, and a bytestring (a path or hash) is the key. I'm using
-- bytestring-sorted input, in this case, so we can easily insert these
-- in a sequential order (though they may be widely scattered).
writeSecondaryIndexes :: VSpace -> MDB_txn -> Map ByteString [Allocation] -> IO ()
writeSecondaryIndexes vc txn idx =
    if (Map.null idx) then return () else
    alloca $ \ pAddr -> do
    let vAddr = MDB_val { mv_data = castPtr pAddr, mv_size = fromIntegral addrSize }
    croot <- mdb_cursor_open' txn (vcache_db_vroots vc) 
    chash <- mdb_cursor_open' txn (vcache_db_caddrs vc)

    -- logic inlined for easy access to cursors
    let insertRoot = compileWriteFlags [MDB_NOOVERWRITE]
    let recordRoot an = 
            withByteStringVal (alloc_name an) $ \ vKey -> do
            poke pAddr (alloc_addr an)
            bOK <- mdb_cursor_put' insertRoot croot vKey vAddr
            unless bOK (fail $ "VCache bug: " ++ show (alloc_name an) ++ " root indexed twice")
    let insertHash = compileWriteFlags [MDB_NODUPDATA]
    let recordHash an = 
            withByteStringVal (alloc_name an) $ \ vKey -> do
            poke pAddr (alloc_addr an)
            bOK <- mdb_cursor_put' insertHash chash vKey vAddr
            unless bOK (addrBug (alloc_addr an) " VRef indexed twice")
    let recordAlloc an =
            if BS.null (alloc_name an) then return () else
            if isPVarAddr (alloc_addr an) then recordRoot an else
            recordHash an

    -- run indexing operation    
    mapM_ (mapM_ recordAlloc) (Map.elems idx) 

    mdb_cursor_close' croot
    mdb_cursor_close' chash
    return ()

-- update reference counts in the database. This requires, for each 
-- address, reading the old reference count then writing a new value. 
--
-- VCache uses two tables for reference counts. One table just contains
-- all addresses with zero references (this simplifies GC). The other
-- table contains addresses with refct values greater than zero. An
-- address will be shifted from one table to the other as needed. 
--
-- Reference counts do not include the ephemeral references held only 
-- by the Haskell process. Those simply prevent zeroes from causing a
-- collection cycle.
--
-- This operation should never fail. Failure indicates there is a bug
-- in VCache or some external source of database corruption. 
--
updateReferenceCounts :: VSpace -> MDB_txn -> Address -> RefctDiff -> IO ()
updateReferenceCounts vc txn allocInit rcDiffMap =
    if Map.null rcDiffMap then return () else
    alloca $ \ pAddr ->
    allocaBytes 8 $ \ pRefctBuff -> 
    alloca $ \ pvAddr ->
    alloca $ \ pvData -> do
    let vAddr = MDB_val { mv_data = castPtr pAddr, mv_size = fromIntegral addrSize }
    let vZero = MDB_val { mv_data = nullPtr, mv_size = 0 }
    poke pvAddr vAddr -- the MDB_SET cursor operations will not update this
    wrc <- mdb_cursor_open' txn (vcache_db_refcts vc) -- write new reference counts
    wc0 <- mdb_cursor_open' txn (vcache_db_refct0 vc) -- write zeroes for ephemeral values

    -- the logic is inlined here for easy access to buffers and cursors
    let appendEntry = compileWriteFlags [MDB_APPEND]
    let newEphemeron addr = do -- just write a zero
            poke pAddr addr -- prepare vAddr and pvAddr
            bOK <- mdb_cursor_put' appendEntry wc0 vAddr vZero
            unless bOK (addrBug addr "refct0 could not be appended")
    let newAllocation addr rc =
            if (0 == rc) then newEphemeron addr else do 
            unless (rc > 0) (addrBug addr "allocated with negative refct")
            vRefct <- writeRefctBytes pRefctBuff rc
            poke pAddr addr -- prepare vAddr and pvAddr
            bOK <- mdb_cursor_put' appendEntry wrc vAddr vRefct
            unless bOK (addrBug addr "refct could not be appended")
    let updateKey = compileWriteFlags []
    let deleteCursor = compileWriteFlags []
    let clearOldZero addr rc = do
            unless (rc > 0) (addrBug addr "has negative refct")
            bZeroFound <- mdb_cursor_get' MDB_SET wc0 pvAddr pvData
            unless bZeroFound (addrBug addr "has undefined refct")
            mdb_cursor_del' deleteCursor wc0 -- delete old zero!
            vRefct <- writeRefctBytes pRefctBuff rc
            _ <- mdb_cursor_put' updateKey wrc vAddr vRefct
            return ()
    let updateCursor = compileWriteFlags [MDB_CURRENT]
    let updateRefct (addr,rcDiff) = 
            if (addr >= allocInit) then newAllocation addr rcDiff else 
            if (rcDiff == 0) then return () else -- skip 0 deltas
            poke pAddr addr >> -- prepare vAddr and pvAddr
            mdb_cursor_get' MDB_SET wrc pvAddr pvData >>= \ bOldRefct ->
            if (not bOldRefct) then clearOldZero addr rcDiff else
            peek pvData >>= readRefctBytes >>= \ rcOld -> 
            let rc = rcOld + rcDiff in
            if (rc < 0) then addrBug addr "updated to negative refct" else
            if (0 == rc) 
                then do mdb_cursor_del' deleteCursor wrc
                        _ <- mdb_cursor_put' updateKey wc0 vAddr vZero
                        return ()
                else do vRefct <- writeRefctBytes pRefctBuff rc
                        _ <- mdb_cursor_put' updateCursor wrc vAddr vRefct
                        return ()

    -- process every reference count update
    mapM_ updateRefct (Map.toAscList rcDiffMap)
    mdb_cursor_close' wc0
    mdb_cursor_close' wrc
    return ()

-- paranoid checks for bugs that should be impossible
addrBug :: Address -> String -> IO a
addrBug addr msg = fail $ "VCache bug: address " ++ show addr ++ " " ++ msg

-- Since we only make one pass through memory, we need to maintain notes
-- about the changes in content:
--
--  * changes in reference counts from content
--  * new references from `newPVar` (below allocator)
--  * hash values for deleted VRefs
-- 
-- That's it for now. I could separate these into multiple passes, but 
-- one goal here is to minimize paging in case of large transactions.
-- 
type UpdateNotes = RefctDiff

-- Update the memory table, and accumulate some information to help
-- with updating reference counts and secondary indices. This pass 
-- is responsible for deleting content selected for GC.
--
updateVirtualMemory :: VSpace -> MDB_txn -> Address -> WriteBatch -> IO UpdateNotes
updateVirtualMemory vc txn allocStart fb = 
    if Map.null fb then return Map.empty else  
    alloca $ \ pAddr ->
    alloca $ \ pvAddr ->
    alloca $ \ pvOldData -> do
    let vAddr = MDB_val { mv_data = castPtr pAddr, mv_size = fromIntegral addrSize }
    poke pvAddr vAddr -- used with MDB_SET so should not be modified
    cmem <- mdb_cursor_open' txn (vcache_db_memory vc) 

    -- logic inlined here for easy access to cursors and buffers

    -- Deletion is marked as writing a null bytestring. Deleted values
    -- must already be on disk or the GC wouldn't notice them. When a
    -- VRef is deleted, we need to manage the hash.
    -- 
    -- if we delete something, it had better exist first.
    let df = compileWriteFlags []
    let delete rcs addr = do
            poke pAddr addr
            bExists <- mdb_cursor_get' MDB_SET cmem pvAddr pvOldData
            unless bExists (addrBug addr "undefined on deletion")
            when (isVRefAddr addr) (fail "VCache todo: delete: hash deleted VRefs")
            vOldData <- peek pvOldData
            oldDeps <- readDataDeps vc addr vOldData
            mdb_cursor_del' df cmem
            return $! addRefcts oldDeps rcs

    -- when we can, we use MDB_APPEND to optimize writes to end of tree
    let cf = compileWriteFlags [MDB_APPEND]
    let create rcs addr bytes =   
            withByteStringVal bytes $ \ vData -> do
            poke pAddr addr 
            bOK <- mdb_cursor_put' cf cmem vAddr vData
            unless bOK (addrBug addr "could not be appended")
            return rcs

    -- for newPVar, our first write may be latent. In this case, we
    -- must ensure the PVar is added to the refct tables.
    let wf = compileWriteFlags []
    let firstWrite rcs addr bytes =
            withByteStringVal bytes $ \ vData -> do
            bOK <- mdb_cursor_put' wf cmem vAddr vData
            unless bOK (addrBug addr "newPVar could not be written")
            fail "VCache todo: firstWrite: record address for refct table"

    let uf = compileWriteFlags [MDB_CURRENT]
    let update rcs addr bytes =
            poke pAddr addr >>
            mdb_cursor_get' MDB_SET cmem pvAddr pvOldData >>= \ bExists ->
            if not bExists then firstWrite rcs addr bytes else 
            withByteStringVal bytes $ \ vData -> do
            oldDeps <- readDataDeps vc addr =<< peek pvOldData
            bOK <- mdb_cursor_put' uf cmem vAddr vData
            unless bOK (addrBug addr "PVar could not updated")
            return $! addRefcts oldDeps rcs

    let processCell rcs (addr, (bytes, deps')) =
            if (BS.null bytes) then assert (L.null deps') $ delete rcs addr else    -- Deletions requested by collector
            if (addr >= allocStart) then create rcs addr bytes else                 -- Allocations requested by allocator
            if (isVRefAddr addr) then addrBug addr "written out of order" else      -- VRefs MUST be written by allocator
            update rcs addr bytes                                                   -- Update, or first write for newPVar

    rcOld <- foldM processCell Map.empty (Map.toAscList fb)
    mdb_cursor_close' cmem

    assertValidOldDeps allocStart rcOld -- sanity check
    let rcNew = Map.foldr' (addRefcts . fmap putChildAddr . snd) Map.empty fb
    let rcDiff = Map.unionWith (-) rcNew rcOld

    return $! rcDiff

addRefcts :: [Address] -> RefctDiff -> RefctDiff
addRefcts = flip (L.foldl' altr) where
    altr rc addr = Map.alter (Just . maybe 1 (+ 1)) addr rc 
{-# INLINABLE addRefcts #-}

-- this is just a paranoid check. It should not fail, unless there is
-- a bug in VCache or similar... or if you manage to allocate all 2^63
-- addresses (which would take a quarter million years at one million
-- allocations per second). 
assertValidOldDeps :: Address -> RefctDiff -> IO ()
assertValidOldDeps allocStart rcDepsOld = 
    case Map.maxViewWithKey rcDepsOld of
        Nothing -> return ()
        Just ((maxOldDep,_), _) -> 
            unless (maxOldDep < allocStart) $ -- should not happen
            addrBug maxOldDep ("found in allocator's space (which starts at " ++ show allocStart ++ ")")

readDataDeps :: VSpace -> Address -> MDB_val -> IO [Address]
readDataDeps vc addr vData = _vget vgetInit state0 >>= toDeps where
    toDeps (VGetR () sf) = return (vget_children sf)
    toDeps (VGetE eMsg) = addrBug addr $ "contains malformed data: " ++ eMsg
    state0 = VGetS
        { vget_children = []
        , vget_target = mv_data vData
        , vget_limit = mv_data vData `plusPtr` fromIntegral (mv_size vData)
        , vget_space = vc
        }


-- GC Constraints:
--
-- VRefs can be revived due to structure sharing. I must arbitrate
-- whether a VRef is GC'd or revived, i.e. via atomic update and
-- tracking GC frames.
--
-- Question: Will I need to combine the Allocator and Collector
-- models into a single IORef? If not, how much lag time between
-- allocation and GC will I require?
--
-- In worst case scenario, we have reader at frame N-2 relative
-- to writer, and the reader reads the allocator whose earliest
-- content corresponds to N-4. So, if I separate Allocator and
-- Collector, I'm needing to protect at about four frames, modulo
-- fencepost errors.
--
-- If I combine the collector and allocator, then it wouldn't be a
-- problem to GC addresses that are still in the allocations table.
-- I only need to protect new allocations, which should be trivial:
-- New allocations won't be in the zeroes table yet.
--
-- So, I'm leaning towards combining these. That gives me about
-- three allocation frames and two GC frames and two ephemeron
-- tables to work with, plus a few addresses at the edges.
--
-- At this point, I have greater contention on this single 
-- structure. It shouldn't be a big problem, since the bulk
-- of computations would still be parallelizable.
-- 



-- This garbage collector step simply searches the database for
-- addresses with zero references, filters for addresses in one
-- of the ephemeron maps, then 
runGarbageCollector :: VSpace -> MDB_txn -> Int -> IO WriteBatch
runGarbageCollector vc txn gcLimit = error "TODO"

gcCell :: WriteCell
gcCell = (BS.empty, [])    




-- How many threads?
--
-- If I have just one writer thread, it would be responsible taking
-- the set of PVar writes, serializing them, then grabbing all the
-- available VRefs... then serializing them, too. The result is long
-- but fully coherent transactions, and potentially some extra latency
-- waiting for serializations in the same thread as the PVar writes.
--
-- If I run the serializers in a separate thread, I might create some
-- VRefs while serializing, and I might get a little parallelism if
-- one thread is writing to disk while the other is preparing a batch
-- of updates. But there is also some risk that I'll create a few 
-- micro-transactions that each involve adding a few VRefs. That will
-- potentially create a lot of extra latency. 
--
-- For synchronization, I also have an option of using a separate
-- thread. Doing so would allow the writer to immediately continue,
-- and might allow multiple generations of writers to overlap wait
-- times. OTOH, the benefit is likely marginal. Due to STM layer,
-- even durable writers don't wait on each other, only on the final
-- write. So, at most, a durable transaction will wait on two frames.
-- For now, I'll keep synchronization coupled to transaction commit.
--
-- To support GC, another thread might be worthwhile, and could also
-- be responsible for clearing the VRef caches and so on. The main 
-- trick, I think, will be to allow the GC thread to halt when there
-- isn't enough work to do.

--
-- An interesting question is whether I can use MDB_APPEND to load 
-- newly allocated content. I think I would like to do so, as it is
-- very efficient and results in tightly compacted storage. Given 
-- that a PVar might be allocated AND updated in the same frame, I
-- might need to recognize this and shift new PVars to the allocation
-- set (i.e. the MDB_APPEND set) for writing, to avoid out-of-order
-- updates.
--

-- Regarding waits: I first wait either on the reader-writer lock or
-- the signal for work to be done. Waiting on the reader-writer lock 
-- has the advantage of shifting new readers into the next frame. The
-- wait on the oldest reader frame cannot be avoided anyway.
--
-- Regarding serialization: I can try either to serialize the PVars
-- incrementally as I write them, or try to serialize as one batch
-- then write them in a separate batch. The batch approach is simpler,
-- and I don't need to think as hard about VRefs constructed during
-- serialization (e.g. due to laziness). The incremental approach
-- would typically have better memory usage properties. OTOH, the
-- incremental approach doesn't really apply to the VRefs anyway. 
-- For now, I'll favor simplicity, and just batch each step.
--
-- Regarding GC: I keep a list of dependencies in the allocator to 
-- prevent GC of old content by a concurrent writer. Within the writer,
-- that doesn't really matter any more. Question: Can I GC recently
-- allocated content? I think I'd rather not: it would complicate the
-- GC greatly if I needed to worry about VRef structure sharing based
-- on values still visible in the allocator that were since GC'd.
-- 

