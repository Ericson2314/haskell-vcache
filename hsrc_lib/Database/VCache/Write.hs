{-# LANGUAGE BangPatterns #-}

-- Implementation of the Writer threads.
--
-- Some general design goals here:
--
--   Avoid writing short-lived 
--   Favor sequential processing (not random access)
--   Single read/write pass per database per frame
--   Append newly written content when possible
--
-- An exception to the single pass is the db_refct0 table, for which
-- I'll make a few passes. Assuming GC is working, db_refct0 should
-- be small. We'll operate on a tight segment of it in any case, so
-- paging shouldn't become a problem.
--
-- The writer handles GC to simplify reasoning about concurrency, in
-- particular the arbitration between reviving VRef addresses via the
-- structure sharing feature and deleting VRef addresses with zero
-- references. GC is incremental to avoid latency spikes with durable
-- transactions.
--
-- KNOWN BUGS: 
--   0.2.7 with the changes, I can get an undefined refct on objects
--     that I probably GC'd earlier in the same frame. This wasn't an
--     issue in 0.2.6 because I held VRef and PVar dependencies until
--     after the write frame so GC would be prevented by mem_vrefs and
--     mem_pvars. In 0.2.7 this is no longer the case.
--
--     I need to prevent GC of dependencies of the current frame.
--
--     Since I already plan to be computing a dependency set for nursery
--     collection, I can probably benefit from using a more 'complete'
--     dependency set to preserve older dependencies instead of just
--     new allocations. I could potentially precompute reference count
--     increases with the same effort. Then I must add an extra constraint:
--     can't GC content in mem_vrefs, mem_pvars, or rcNew.
--     
--     I could then skip reading newDeps in the updater.
--     
-- 
-- 
module Database.VCache.Write
    ( writeStep
    ) where

import Control.Monad
import Control.Exception
import Control.Concurrent
import Control.Concurrent.STM
import Data.IORef
import Data.Maybe
import Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.List as L
import Data.Traversable (traverse)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified System.Mem as Mem
import Foreign.Ptr
import Foreign.Storable
import Foreign.Marshal.Alloc

import Database.LMDB.Raw
import Database.VCache.Types 
import Database.VCache.VPutFini -- serialize updated PVars
import Database.VCache.VGetInit -- read dependencies to manage refcts
import Database.VCache.RWLock -- need a writer lock
import Database.VCache.Refct  -- to update reference counts
import Database.VCache.Hash   -- for GC of VRefs
import Database.VCache.Aligned

-- | when processing write batches, we'll need to track
-- differences in reference counts for each address.
type RefctDiff = Map Address Refct

-- A batch of writes to perform, or will contain a null bytestring to
-- indicate destruction. (Null bytestrings are not valid outputs due
-- to recording at least the size of the address list.) 
type WriteBatch = Map Address ByteString
type GCBatch = WriteBatch

-- For updating secondary indices, track names to addresses.
-- In this case, we use the nursery reference: older addresses
-- are deleted, while newer addresses are inserted.
type UpdSeek = Map ByteString [Address]

addrSize :: Int
addrSize = sizeOf (undefined :: Address)

-- Single step for VCache writer.
writeStep :: VSpace -> IO ()
writeStep vc = withRWLock (vcache_rwlock vc) $ do
    takeMVar (vcache_signal vc) 

    -- prepare update batch (allocations, writes)
    (Writes wlog wsync) <- atomically (takeWrites (vcache_writes vc))
    pvb <- seralizeWrites vc wlog -- PVar states as bytestrings
    Mem.performMinorGC -- nursery GC of Haskell memory

    afrm <- allocFrameStep vc
    let allocInit = alloc_init afrm
    let ub = Map.union pvb (alloc_list afrm)

    -- prepare LMDB-layer read-write transaction.
    txn <- mdb_txn_begin (vcache_db_env vc) Nothing False

    -- select zero-refct addresses for garbage collection 
    let gcLimit = 1000 + (2 * Map.size ub) -- adaptive GC rate
    gcb <- runGarbageCollector vc txn gcLimit

    -- update the db_memory
    let fb = Map.union gcb ub -- full batch, writes & deletes
    let bUpdGCSep = Map.size fb == (Map.size ub + Map.size gcb) 
    unless bUpdGCSep (fail "VCache bug: overlapping GC and update targets")
    (UpdateNotes rcDiff hsDel) <- updateVirtualMemory vc txn allocInit fb

    -- Update reference counts. zeroes for new allocations. +1 for roots.
    let lRoots = fmap fst (alloc_root afrm)
    let rcAlloc = addRefcts lRoots $ fmap (const 0) (alloc_list afrm)
    let rcUpd = Map.unionWith (+) rcDiff rcAlloc
    updateReferenceCounts vc txn allocInit rcUpd

    -- Update VRef hashes 
    let hsAlloc = alloc_seek afrm
    let hsUpd = Map.unionWith (++) hsDel hsAlloc
    updateContentAddressTable vc txn allocInit hsUpd

    -- add new Root PVar names (if any)
    insertRootPVars vc txn (alloc_root afrm)

    -- Finish writeStep: commit, synch, stats, signals.
    mdb_txn_commit txn -- LMDB commit & synch
    modifyIORef' (vcache_gc_count vc) (+ (Map.size gcb)) -- update GC count
    forM_ wsync syncSignal -- signal waiting threads
{-# NOINLINE writeStep #-}

{-
-- TODO: I would like to perform a 'nursery GC' of the allocator, such
-- that transient nodes in a trie are never written to LMDB. 
--
-- Roots:
--  new named root PVars are true roots
--  PVars older than nursery are effective roots
--  addresses held by mem_vrefs or mem_pvars are effective roots
--
-- I can compute a root set, filter  
--
-- What I'd like to do is filter the allocator frame so it simply does
-- not contain the content that was not written, including alloc_seek.
--
stepWriteFrame :: VSpace -> WriteBatch -> IO AllocFrame
-}

-- Shift the allocation frame. This is safe once per write frame.
-- By preserving recent writes in memory for a couple frames, we
-- ensure structure sharing for recently allocated content.
allocFrameStep :: VSpace -> IO AllocFrame
allocFrameStep vc = modifyMVarMasked (vcache_memory vc) $ \ m -> do
    let ac = mem_alloc m
    let addr = alloc_new_addr ac
    let ac' = Allocator
            { alloc_new_addr = addr
            , alloc_frm_next = AllocFrame Map.empty Map.empty [] addr
            , alloc_frm_curr = alloc_frm_next ac
            , alloc_frm_prev = alloc_frm_curr ac
            -- old alloc_frm_prev is GC'd.
            }
    let m' = m { mem_alloc = ac' }
    return (m', alloc_frm_curr ac')

takeWrites :: TVar Writes -> STM Writes
takeWrites tv = do
    wb <- readTVar tv
    writeTVar tv (Writes Map.empty [])
    return wb

seralizeWrites :: VSpace -> WriteLog -> IO WriteBatch
seralizeWrites = traverse . writeTxW

writeTxW :: VSpace -> TxW -> IO ByteString
writeTxW vc (TxW wf v) = fmap snd (runVPutIO vc (wf v))

syncSignal :: MVar () -> IO ()
syncSignal = void . flip tryPutMVar ()

-- Write new PVar roots. This is a rare event, so doesn't need to be
-- significantly optimized.
insertRootPVars :: VSpace -> MDB_txn -> [(Address,ByteString)] -> IO ()
insertRootPVars vc txn rootList = 
    if (L.null rootList) then return () else
    alloca $ \ pAddr -> do
    let vAddr = MDB_val { mv_data = castPtr pAddr, mv_size = fromIntegral addrSize }
    croot <- mdb_cursor_open' txn (vcache_db_vroots vc) 
    let flags = compileWriteFlags [MDB_NOOVERWRITE]
    let insertRoot (addr, path) = 
            withByteStringVal path $ \ vKey -> do
            poke pAddr addr
            bOK <- mdb_cursor_put' flags croot vKey vAddr
            unless bOK (fail "VCache bug: attempt to overwrite named root")
    mapM_ insertRoot rootList
    mdb_cursor_close' croot
{-# NOINLINE insertRootPVars #-}

-- update content addressing table to support structure sharing of VRefs.
-- Hashes are inserted sequentially, but may be highly distributed in the
-- table. The benefits of sequential insert are probably marginal.
updateContentAddressTable :: VSpace -> MDB_txn -> Address -> UpdSeek -> IO ()
updateContentAddressTable vc txn allocInit updSeek =
    if (Map.null updSeek) then return () else
    alloca $ \ pAddr -> 
    alloca $ \ pvKey ->
    alloca $ \ pvAddr -> do
    let vAddr = MDB_val { mv_data = castPtr pAddr, mv_size = fromIntegral addrSize }
    chash <- mdb_cursor_open' txn (vcache_db_caddrs vc)

    -- logic inlined for easy access to cursors and buffers
    let insertHash vKey addr = do
            let flags = compileWriteFlags [MDB_NODUPDATA]
            bOK <- mdb_cursor_put' flags chash vKey vAddr
            unless bOK (addrBug addr "VRef hash recorded twice")
    let deleteHash vKey addr = do
            poke pvKey vKey
            poke pvAddr vAddr
            bExist <- mdb_cursor_get' MDB_GET_BOTH chash pvKey pvAddr -- position cursor
            unless bExist (addrBug addr "VRef hash not found for deletion")
            let flags = compileWriteFlags []
            mdb_cursor_del' flags chash
    let processHash (_hash, addrs) =
            withByteStringVal _hash $ \ vKey ->
            forM_ addrs $ \ addr ->
            poke pAddr addr >> -- prepares vAddr
            if addr < allocInit then deleteHash vKey addr 
                                else insertHash vKey addr

    mapM_ processHash (Map.toAscList updSeek) 
    mdb_cursor_close' chash
{-# NOINLINE updateContentAddressTable #-}

-- | Update reference counts in the database. This requires, for each
-- older address, reading the old reference count, updating it, then
-- writing the new value. Newer addresses may simply be appended. 
--
-- VCache uses two tables for reference counts. One table just contains
-- zeroes. The other table includes positive counts. This separation 
-- makes it easy for the garbage collector to discover its targets.
--
-- I assume that all entries older than allocInit should be recorded
-- in the database, i.e. it's an error for both db_refct and db_refct0
-- to be undefined unless I'm allocating a new address.
--
-- Ephemerons in the Haskell layer are not reference counted.
--
-- This operation should never fail. Failure indicates there is a bug
-- in VCache or some external source of database corruption. 
--
updateReferenceCounts :: VSpace -> MDB_txn -> Address -> RefctDiff -> IO ()
updateReferenceCounts vc txn allocInit rcDiffMap =
    if Map.null rcDiffMap then return () else
    alloca $ \ pAddr ->
    allocaBytes 16 $ \ pRefctBuff -> -- overkill, but that's okay
    alloca $ \ pvAddr ->
    alloca $ \ pvData -> do
    let vAddr = MDB_val { mv_data = castPtr pAddr, mv_size = fromIntegral addrSize }
    let vZero = MDB_val { mv_data = nullPtr, mv_size = 0 }
    poke pvAddr vAddr -- the MDB_SET cursor operations will not update this
    wrc <- mdb_cursor_open' txn (vcache_db_refcts vc) -- write new reference counts
    wc0 <- mdb_cursor_open' txn (vcache_db_refct0 vc) -- write zeroes for ephemeral values

    -- the logic is inlined here for easy access to buffers and cursors
    let newEphemeron addr = do -- just write a zero
            let flags = compileWriteFlags [MDB_APPEND]
            bOK <- mdb_cursor_put' flags wc0 vAddr vZero
            unless bOK (addrBug addr "refct0 could not be appended")
    let newAllocation addr rc =
            if (0 == rc) then newEphemeron addr else do 
            unless (rc > 0) (addrBug addr "allocation with negative refct")
            vRefct <- writeRefctBytes pRefctBuff rc
            let flags = compileWriteFlags [MDB_APPEND]
            bOK <- mdb_cursor_put' flags wrc vAddr vRefct
            unless bOK (addrBug addr "refct could not be appended")
    let updateFromZero addr rc = do
            bZeroFound <- mdb_cursor_get' MDB_SET wc0 pvAddr pvData
            unless bZeroFound (addrBug addr "has undefined refct")
            unless (rc > 0) (addrBug addr "update refct0 to negative refct")
            let df = compileWriteFlags []
            mdb_cursor_del' df wc0
            vRefct <- writeRefctBytes pRefctBuff rc
            let wf = compileWriteFlags [MDB_NOOVERWRITE]
            bOK <- mdb_cursor_put' wf wrc vAddr vRefct
            unless bOK (addrBug addr "could not update refct from zero")
    let updateRefct (addr,rcDiff) = 
            poke pAddr addr >> -- prepares vAddr, pvAddr
            if (addr >= allocInit) then newAllocation addr rcDiff else
            if (0 == rcDiff) then return () else -- zero delta, may skip
            mdb_cursor_get' MDB_SET wrc pvAddr pvData >>= \ bHasRefct ->
            if (not bHasRefct) then updateFromZero addr rcDiff else
            peek pvData >>= readRefctBytes >>= \ rcOld ->
            assert (rcOld > 0) $ 
            let rc = rcOld + rcDiff in
            if (rc < 0) then addrBug addr "positive to negative refct" else
            if (0 == rc) 
                then do let df = compileWriteFlags []
                        mdb_cursor_del' df wrc
                        let wf0 = compileWriteFlags [MDB_NOOVERWRITE]
                        bOK <- mdb_cursor_put' wf0 wc0 vAddr vZero
                        unless bOK (addrBug addr "has both refct0 and refct")
                else do vRefct <- writeRefctBytes pRefctBuff rc
                        let ucf = compileWriteFlags [MDB_CURRENT]
                        bOK <- mdb_cursor_put' ucf wrc vAddr vRefct
                        unless bOK (addrBug addr "could not update refct")

    -- process every reference count update
    mapM_ updateRefct (Map.toAscList rcDiffMap)
    mdb_cursor_close' wc0
    mdb_cursor_close' wrc
    return ()
{-# NOINLINE updateReferenceCounts #-}

-- paranoid checks for bugs that should be impossible
addrBug :: Address -> String -> IO a
addrBug addr msg = fail $ "VCache bug: address " ++ show addr ++ " " ++ msg

-- Since we only make one pass through memory, we need to maintain notes
-- about the changes in content:
--
--  * changes in reference counts from content
--  * hash values for deleted VRefs
-- 
data UpdateNotes = UpdateNotes !RefctDiff !UpdSeek

emptyNotes :: UpdateNotes
emptyNotes = UpdateNotes Map.empty Map.empty

-- create, update, or delete objects in the primary memory table
updateVirtualMemory :: VSpace -> MDB_txn -> Address -> WriteBatch -> IO UpdateNotes
updateVirtualMemory vc txn allocStart fb = 
    if Map.null fb then return emptyNotes else  
    alloca $ \ pAddr ->
    alloca $ \ pvAddr ->
    alloca $ \ pvOldData -> do
    let vAddr = MDB_val { mv_data = castPtr pAddr, mv_size = fromIntegral addrSize }
    poke pvAddr vAddr -- used with MDB_SET so should not be modified
    cmem <- mdb_cursor_open' txn (vcache_db_memory vc) 

    -- logic inlined here for easy access to cursors and buffers
    let create (UpdateNotes rcs hs) addr bytes =
            withByteStringVal bytes $ \ vData -> do
            newDeps <- readDataDeps vc addr vData
            let rcs' = addRefcts newDeps rcs 
            let cf = compileWriteFlags [MDB_APPEND]
            bOK <- mdb_cursor_put' cf cmem vAddr vData
            unless bOK (addrBug addr "created out of order")
            return (UpdateNotes rcs' hs)

    let update (UpdateNotes rcs hs) addr bytes = 
            withByteStringVal bytes $ \ vData -> do
            unless (isPVarAddr addr) (addrBug addr "VRef cannot be updated")
            bExists <- mdb_cursor_get' MDB_SET cmem pvAddr pvOldData
            unless bExists (addrBug addr "undefined on update")
            oldDeps <- readDataDeps vc addr =<< peek pvOldData
            newDeps <- readDataDeps vc addr vData
            let rcs' = (addRefcts newDeps . subRefcts oldDeps) rcs
            let uf = compileWriteFlags [MDB_CURRENT]
            bOK <- mdb_cursor_put' uf cmem vAddr vData
            unless bOK (addrBug addr "PVar update failure")
            return (UpdateNotes rcs' hs)

    let delete (UpdateNotes rcs hs) addr = do
            bExists <- mdb_cursor_get' MDB_SET cmem pvAddr pvOldData
            unless bExists (addrBug addr "undefined on delete")
            vOldData <- peek pvOldData
            hs' <- if not (isVRefAddr addr) then return hs else
                   hashVal vOldData >>= \ h ->
                   return (addHash h addr hs)
            oldDeps <- readDataDeps vc addr vOldData
            let rcs' = subRefcts oldDeps rcs
            let df = compileWriteFlags []
            mdb_cursor_del' df cmem
            return (UpdateNotes rcs' hs')

    let processCell udn (addr, bytes) =
            poke pAddr addr >> -- 
            if (BS.null bytes) then delete udn addr else
            if (addr >= allocStart) then create udn addr bytes 
                                    else update udn addr bytes

    notes <- foldM processCell emptyNotes (Map.toAscList fb)
    mdb_cursor_close' cmem
    return notes
{-# NOINLINE updateVirtualMemory #-}

-- here we might have one bytestring to many addresses... but this is 
-- extremely unlikely.
addHash :: ByteString -> Address -> UpdSeek -> UpdSeek
addHash h addr = Map.alter f h where
    f = Just . (:) addr . fromMaybe [] 

-- subtract 1 from each address refct
subRefcts :: [Address] -> RefctDiff -> RefctDiff
subRefcts = flip (L.foldl' altr) where
    altr = flip $ Map.alter (Just . maybe (-1) (subtract 1))

-- add 1 to each address refct
addRefcts :: [Address] -> RefctDiff -> RefctDiff
addRefcts = flip (L.foldl' altr) where
    altr = flip $ Map.alter (Just . maybe 1 (+ 1))

-- Read just enough of an MDB_val to obtain the address list.
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


-- | Garbage collection in VCache involves selecting addresses with
-- zero references, filtering objects that are held by VRefs and 
-- PVars in Haskell memory, then deleting the remainders. 
--
-- GC is incremental. We limit the amount of work performed in each
-- write step to avoid creating too much latency for writers. The
-- cache manager may initiate extra write cycles to force more GC.
--
-- This garbage collector will immediately delete addresses from
-- the db_refct0 table.
--
runGarbageCollector :: VSpace -> MDB_txn -> Int -> IO GCBatch
runGarbageCollector vc txn gcLimit = do
    gcb0 <- gcCandidates vc txn gcLimit
    gcb <- gcSelectFrame vc gcb0
    gcClearFrame vc txn gcb
    return gcb
{-# NOINLINE runGarbageCollector #-}


gcCandidates :: VSpace -> MDB_txn -> Int -> IO GCBatch
gcCandidates vc txn gcLimit =
    alloca $ \ pvAddr -> do
    c0 <- mdb_cursor_open' txn (vcache_db_refct0 vc)

    let loop !n !b !gcb = -- select candidates
            if (not b) then restartGC vc >> return gcb else
            (peek pvAddr >>= peekAddr) >>= \ addr ->
            let gcb' = Map.insert addr gcCell gcb in
            if (0 == n) then continueGC vc addr >> return gcb' else
            mdb_cursor_get' MDB_NEXT c0 pvAddr nullPtr >>= \ b' ->
            loop (n-1) b' gcb'

    let initC0 = -- continue GC or start from beginning of map
            readIORef (vcache_gc_start vc) >>= \ mbContinue ->
            case mbContinue of
                Nothing -> mdb_cursor_get' MDB_FIRST c0 pvAddr nullPtr
                Just addr -> alloca $ \ pAddr -> do
                    let vAddr = MDB_val { mv_data = castPtr pAddr
                                        , mv_size = fromIntegral $ sizeOf addr }
                    poke pAddr (1 + addr)
                    poke pvAddr vAddr
                    mdb_cursor_get' MDB_SET_RANGE c0 pvAddr nullPtr

    b0 <- initC0
    gcb <- loop (gcLimit - 1) b0 Map.empty
    mdb_cursor_close' c0
    return gcb

-- filter candidates for ephemeral addresses then record the GC frame.
gcSelectFrame :: VSpace -> GCBatch -> IO GCBatch
gcSelectFrame vc gcb = 
    modifyMVarMasked (vcache_memory vc) $ \ m -> do
    let gcb' = ((gcb `Map.difference` mem_vrefs m) 
                     `Map.difference` mem_pvars m) 
    let gc' = GC { gc_frm_curr = GCFrame gcb'
                 , gc_frm_prev = gc_frm_curr (mem_gc m) }
    let m' = m { mem_gc = gc' }
    return (m', gcb')

-- delete GC'd addresses from the db_refct0 table. Returns 
-- number of addresses in 
gcClearFrame :: VSpace -> MDB_txn -> GCBatch -> IO ()
gcClearFrame vc txn gcb = 
    alloca $ \ pAddr -> 
    alloca $ \ pvAddr -> do
    let vAddr = MDB_val { mv_data = castPtr pAddr, mv_size = fromIntegral addrSize }
    poke pvAddr vAddr
    c0 <- mdb_cursor_open' txn (vcache_db_refct0 vc)
    
    let clearAddr addr = do
            poke pAddr addr
            bFound <- mdb_cursor_get' MDB_SET c0 pvAddr nullPtr
            unless bFound (addrBug addr "not found for GC")
            let flags = compileWriteFlags []
            mdb_cursor_del' flags c0

    mapM_ clearAddr (Map.keys gcb)
    mdb_cursor_close' c0
    return ()
   
 
-- GC from first address (affects next frame)
restartGC :: VSpace -> IO ()
restartGC vc = writeIORef (vcache_gc_start vc) Nothing

-- GC from given address (affects next frame)
continueGC :: VSpace -> Address -> IO ()
continueGC vc !addr = writeIORef (vcache_gc_start vc) (Just addr)

gcCell :: ByteString
gcCell = BS.empty    

peekAddr :: MDB_val -> IO Address
peekAddr v =
    let expectedSize = fromIntegral addrSize in
    let bBadSize = expectedSize /= mv_size v in
    if bBadSize then fail "VCache bug: badly formed address" else
    peekAligned (castPtr (mv_data v))
{-# INLINABLE peekAddr #-}

