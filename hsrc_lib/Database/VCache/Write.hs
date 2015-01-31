-- Implementation of the Writer threads.
module Database.VCache.Write
    ( initWriterThreads
    ) where

import Control.Monad
import Control.Exception
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad.Trans.State.Strict
import Data.IORef
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.IntMap.Strict as IntMap
import qualified Data.List as L
import Data.Maybe
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Foreign.Ptr
import Foreign.Storable
import Foreign.Marshal.Alloc

import qualified System.IO as Sys
import qualified System.Exit as Sys

import Database.LMDB.Raw
import Database.VCache.Types
import Database.VCache.VPutFini
import Database.VCache.VGetInit
import Database.VCache.RWLock

-- a write batch records, for each address, both some content 
-- and a list of addresses to incref
type WriteBatch = Map Address WriteCell
type WriteCell = (ByteString, [Address])

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


-- | Create the writer thread(s). These threads run in the background
-- and are responsible for pushing data from PVars to the LMDB layer. 
initWriterThreads :: VSpace -> IO ()
initWriterThreads = task . writerStep where
    task step = void (forkIO (forever step `catch` onE))
    onE :: SomeException -> IO ()
    onE e | isBlockedOnMVar e = return ()
    onE e = do
        putErrLn "VCache writer thread has failed."
        putErrLn (indent "  " (show e))
        putErrLn "Halting program."
        Sys.exitFailure

isBlockedOnMVar :: (Exception e) => e -> Bool
isBlockedOnMVar = isJust . test . toException where
    test :: SomeException -> Maybe BlockedIndefinitelyOnMVar
    test = fromException

putErrLn :: String -> IO ()
putErrLn = Sys.hPutStrLn Sys.stderr

indent :: String -> String -> String
indent ws = (ws ++) . indent' where
    indent' ('\n':s) = '\n' : ws ++ indent' s
    indent' (c:s) = c : indent' s
    indent' [] = []

-- Single step for VCache writer.
writerStep :: VSpace -> IO ()
writerStep vc = withRWLock (vcache_rwlock vc) $ do
    takeMVar (vcache_signal vc) 
    ws <- atomically (takeWrites (vcache_writes vc))
    wb <- batchWrites (write_data ws)
    af <- atomicModifyIORef (vcache_allocator vc) allocFrameStep
    let ab = Map.map fnWriteAlloc (alloc_list af) -- Map Address ByteString
    let fb = Map.union wb ab -- favor written data over allocations
    let allocInit = alloc_init af 
    wtx <- mdb_txn_begin (vcache_db_env vc) Nothing False  -- read-write 
    rtx <- mdb_txn_begin (vcache_db_env vc) Nothing True   -- read-only

    -- record the secondary index updates
    writeSecondaryIndexes vc wtx (Map.elems (alloc_list af))

    -- update the reference counts
    let rcInc = batchRefcts fb
    rcDec <- readDeps vc rtx (L.takeWhile (< allocInit) $ Map.keys fb)
    let rcDiff = Map.unionWith (-) rcInc rcDec
    updateReferenceCounts vc wtx rtx rcDiff

    -- update the virtual memory
    updateVirtualMemory vc wtx (alloc_init af) fb

    -- perform garbage collection
    runGarbageCollector vc wtx rtx (alloc_init af)

    -- finish up
    mdb_txn_abort  rtx
    mdb_txn_commit wtx
    mapM_ syncSignal (write_sync ws)


takeWrites :: TVar Writes -> STM Writes
takeWrites tv = do
    wb <- readTVar tv
    writeTVar tv (Writes Map.empty [])
    return wb

batchWrites :: WriteLog -> IO WriteBatch
batchWrites = Map.traverseWithKey (const writeTxW)
{-# INLINE batchWrites #-}

writeTxW :: TxW -> IO WriteCell
writeTxW (TxW pv v) =
    runVPutIO (pvar_space pv) (pvar_write pv v) >>= \ ((), _data, _deps) ->
    return (_wdd _data _deps)

fnWriteAlloc :: Allocation -> WriteCell
fnWriteAlloc an = _wdd (alloc_data an) (alloc_deps an)

_wdd :: ByteString -> [PutChild] -> WriteCell
_wdd _data _deps = (_data, _addr) where
    _addr = fmap putChildAddr _deps
{-# INLINE _wdd #-}

-- Reviewing safety one more time:
--
--   allocFrameStep runs under the writer lock, so older readers are gone.
--   More precisely, current readers are operating on frames N-1 or N-2
--
--   However, readers still exist for the two frames previous to whatever
--   the writer is working on.
--   
-- allocFrameStep runs under the read-write lock, which
-- means that the writer has already waited for all readers
-- from frame N-2 to finish. 
allocFrameStep :: Allocator -> (Allocator, AllocFrame)
allocFrameStep ac =
    let thisFrame = alloc_frm_next ac in
    let prevFrame = alloc_frm_curr ac in
    let newFrame = AllocFrame 
            { alloc_init = alloc_new_addr ac
            , alloc_list = Map.empty
            , alloc_seek = IntMap.empty
            }
    in
    let newAC = Allocator 
            { alloc_new_addr = (alloc_new_addr ac)
            , alloc_frm_next = newFrame
            , alloc_frm_curr = thisFrame
            , alloc_frm_prev = prevFrame
            }
    in
    (newAC, thisFrame)

syncSignal :: MVar () -> IO ()
syncSignal mv = void (tryPutMVar mv ())


-- Write the PVar roots and VRef hashmap. In this case the order of 
-- writes is effectively random and may be very widely scattered. I
-- will not bother with cursors. This is only performed for fresh
-- allocations, so there should never be any risk of overwrite.
--
-- This operation should never fail. If failure is detected, the 
-- database has somehow been corrupted.
writeSecondaryIndexes :: VSpace -> MDB_txn -> [Allocation] -> IO ()
writeSecondaryIndexes vc wtx = mapM_ recordAlloc where
    recordAlloc an =
        if (BS.null (alloc_name an)) then return () else
        if isPVarAddr (alloc_addr an) then recordPVar an else
        recordVRef an
    rootFlags = compileWriteFlags [MDB_NOOVERWRITE] -- don't change PVar data
    recordPVar an =
        withByteStringVal (alloc_name an) $ \ vKey ->
        withAddrVal (alloc_addr an) $ \ vData -> 
        mdb_put' rootFlags wtx (vcache_db_vroots vc) vKey vData >>= \ kExist ->
        when kExist (fail $ "VCache bug: root " ++ show (alloc_name an) ++ " created twice")
    hashFlags = compileWriteFlags [MDB_NODUPDATA] -- allocated VRef should not already exist
    recordVRef an =
        withByteStringVal (alloc_name an) $ \ vKey ->
        withAddrVal (alloc_addr an) $ \ vData ->
        mdb_put' hashFlags wtx (vcache_db_caddrs vc) vKey vData >>= \ kvExist ->
        when kvExist (fail $ "VCache bug: VRef#" ++ show (alloc_addr an) ++ " written twice")


-- when processing write batches, we'll need to track
-- differences in reference counts for each address.
type WithRefct = StateT RefCtDiff
type RefCtDiff = Map Address Int

incRefs :: (Monad m) => [Address] -> WithRefct m ()
incRefs = modify . addRefCts
{-# INLINE incRefs #-}

addRefCts :: [Address] -> RefCtDiff -> RefCtDiff
addRefCts = flip (L.foldl' altr) where
    altr rc addr = Map.alter (Just . maybe 1 (+ 1)) addr rc 
{-# INLINABLE addRefCts #-}

withAddrVal :: Address -> (MDB_val -> IO a) -> IO a
withAddrVal addr action = alloca $ \ pAddr -> do
    poke pAddr addr
    action $ MDB_val { mv_data = castPtr pAddr
                     , mv_size = fromIntegral (sizeOf addr) 
                     }
{-# INLINE withAddrVal #-}

-- compute reference counts associated with a write batch.
batchRefcts :: WriteBatch -> RefCtDiff 
batchRefcts = L.foldl' (flip addRefCts) Map.empty . fmap snd . Map.elems
{-# INLINABLE batchRefcts #-}

-- read dependencies for list of addresses into database. This requires a 
-- partial read for each involved element, i.e. just vgetInit. A cursor
-- is used, thus fastest if addresses are provided in sorted order.
--
-- It's an error if any of these addresses are not defined or cannot be
-- read. These should not be freshly allocated addresses.
readDeps :: VSpace -> MDB_txn -> [Address] -> IO RefCtDiff
readDeps vc rtx addrs =
    fail "VCache todo: read reference counts used by each updated address"

-- update reference counts in the database. This requires, for each 
-- address, reading the old reference count and writing a new value. 
updateReferenceCounts :: VSpace -> MDB_txn -> MDB_txn -> RefCtDiff -> IO ()
updateReferenceCounts vc wtx rtx rcDiff =
    fail "VCache todo: update reference counts for each address"

-- Primary code for updating the virtual memory. This writes in two
-- phases: first, it updates existing content, then writes any new
-- content to the end of the address space. This operation does not
-- attempt to read anything.
updateVirtualMemory :: VSpace -> MDB_txn -> Address -> WriteBatch -> IO ()
updateVirtualMemory vc wtx allocStart fb = 
    fail "VCache todo: update memory"


runGarbageCollector :: VSpace -> MDB_txn -> MDB_txn -> Address -> IO ()
runGarbageCollector vc wtx rtx ub =
    fail "TODO: garbage collection"

