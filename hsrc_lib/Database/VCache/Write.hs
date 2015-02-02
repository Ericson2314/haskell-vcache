-- Implementation of the Writer threads.
module Database.VCache.Write
    ( initWriterThreads
    ) where

import Control.Monad
import Control.Exception
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad.Trans.State.Strict
import Data.Bits
import Data.IORef
import Data.Word
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

-- when processing write batches, we'll need to track
-- differences in reference counts for each address.
type RefctDiff = Map Address Refct
type Refct = Int


addrSize :: Int
addrSize = sizeOf (undefined :: Address)

-- | Create the writer thread(s). These threads run in the background
-- and are responsible for pushing data from PVars to the LMDB layer. 
initWriterThreads :: VSpace -> IO ()
initWriterThreads vc = begin where
    begin = do
        task (writerStep vc)
    task step = void (forkIO (forever step `catch` onE))
    onE :: SomeException -> IO ()
    onE e | isBlockedOnMVar e = return () -- full GC of VCache
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
    wb <- seralizeWrites (write_data ws)
    af <- atomicModifyIORef (vcache_allocator vc) allocFrameStep
    let ab = Map.map fnWriteAlloc (alloc_list af) -- Map Address ByteString
    let fb = Map.union wb ab -- favor written data over allocations
    let allocInit = alloc_init af 
    txn <- mdb_txn_begin (vcache_db_env vc) Nothing False

    -- update reference counts.
    rcDepsOld <- readDeps vc txn (L.takeWhile (< allocInit) $ Map.keys fb)      -- lost references from old content
    let rcDepsNew = Map.foldr' (addRefcts . snd) Map.empty fb                   -- new references from new content
    let rcAlloc = fmap (\ an -> if isNewRoot an then 1 else 0) (alloc_list af)  -- new references from allocations
    let rcDepsDiff = Map.filter (/= 0) $ Map.unionWith (-) rcDepsNew rcDepsOld  -- minimal delta for content refcts
    let rcBatch = Map.unionWith (+) rcDepsDiff rcAlloc                          -- plus references for allocations
    assertValidOldDeps allocInit rcDepsOld                                      -- simple sanity check
    updateReferenceCounts vc txn allocInit rcBatch                              -- write batch of reference counts

    -- record secondary indexes: PVar roots, VRef hashes
    writeSecondaryIndexes vc txn (Map.elems (alloc_list af))

    -- update the virtual memory
    updateVirtualMemory vc txn allocInit fb

    -- perform garbage collection
    runGarbageCollector vc txn allocInit 

    -- finish up
    mdb_txn_commit txn
    mapM_ syncSignal (write_sync ws)

isNewRoot :: Allocation -> Bool
isNewRoot an = isPVarAddr (alloc_addr an) && not (BS.null (alloc_name an))

takeWrites :: TVar Writes -> STM Writes
takeWrites tv = do
    wb <- readTVar tv
    writeTVar tv (Writes Map.empty [])
    return wb

seralizeWrites :: WriteLog -> IO WriteBatch
seralizeWrites = Map.traverseWithKey (const writeTxW)
{-# INLINE seralizeWrites #-}

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
--   More precisely, current readers are operating on LMDB frames N-1 or 
--   N-2, where frame N is the frame the writer is working on. 
--
--   Readers at N-2 can see any content written by the writer at N-2. But
--   they do need access to content written at frames N-1, N, or delayed
--   to the next writer frame. 
--
--   Thus, the writer at frame N is free to shift content written in frame
--   N into the alloc_frm_curr, while alloc_frm_next becomes the N+1 frame
--   and alloc_frm_prev refers to the N-1 frame. Content from the old 
--   alloc_frm_prev may be garbage collected.
--
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


-- Write the PVar roots and VRef hashmap. 
--
-- In this case the order of writes is effectively random and may
-- be very widely scattered, so I'm not going to bother with 
-- optimizing the ordering. 
--
-- This operation should never fail. If failure is detected, the 
-- database has been somehow been corrupted.
writeSecondaryIndexes :: VSpace -> MDB_txn -> [Allocation] -> IO ()
writeSecondaryIndexes vc txn = mapM_ recordAlloc where
    recordAlloc an =
        if (BS.null (alloc_name an)) then return () else
        if isPVarAddr (alloc_addr an) then recordPVar an else
        recordVRef an
    rootFlags = compileWriteFlags [MDB_NOOVERWRITE] -- root PVar address must be stable.
    recordPVar an =
        withByteStringVal (alloc_name an) $ \ vKey ->
        withAddrVal (alloc_addr an) $ \ vData -> 
        mdb_put' rootFlags txn (vcache_db_vroots vc) vKey vData >>= \ kExist ->
        when kExist (fail $ "VCache bug: root " ++ show (alloc_name an) ++ " created twice")
    hashFlags = compileWriteFlags [MDB_NODUPDATA] -- allocated VRef should not already exist
    recordVRef an =
        withByteStringVal (alloc_name an) $ \ vKey ->
        withAddrVal (alloc_addr an) $ \ vData ->
        mdb_put' hashFlags txn (vcache_db_caddrs vc) vKey vData >>= \ kvExist ->
        when kvExist (fail $ "VCache bug: VRef#" ++ show (alloc_addr an) ++ " written twice")

addRefcts :: [Address] -> RefctDiff -> RefctDiff
addRefcts = flip (L.foldl' altr) where
    altr rc addr = Map.alter (Just . maybe 1 (+ 1)) addr rc 
{-# INLINABLE addRefcts #-}

withAddrVal :: Address -> (MDB_val -> IO a) -> IO a
withAddrVal addr action = alloca $ \ pAddr -> do
    poke pAddr addr
    action $ MDB_val { mv_data = castPtr pAddr
                     , mv_size = fromIntegral (sizeOf addr) 
                     }
{-# INLINE withAddrVal #-}

-- read dependencies for list of addresses into database. This requires a 
-- partial read for each involved element, i.e. just vgetInit. A cursor
-- is used, thus fastest if addresses are provided in sorted order.
--
-- It's an error if any of these addresses are not defined or cannot be
-- read. These should not be freshly allocated addresses.
readDeps :: VSpace -> MDB_txn -> [Address] -> IO RefctDiff
readDeps _ _ [] = return Map.empty
readDeps vc txn addrs = 
    alloca $ \ pAddr -> 
    alloca $ \ pvAddr ->
    alloca $ \ pvData -> do
    poke pvAddr $ MDB_val { mv_data = castPtr pAddr, mv_size = fromIntegral addrSize }
    rdMem <- mdb_cursor_open' txn (vcache_db_memory vc) 
    let addrDeps rcd addr = do
            poke pAddr addr
            bFound <- mdb_cursor_get' MDB_SET rdMem pvAddr pvData
            unless bFound (addrBug addr "is not defined")
            vData <- peek pvData
            depAddrs <- readDataDeps vc addr vData
            return $! (addRefcts depAddrs rcd)
    rcDiffs <- foldM addrDeps Map.empty addrs
    mdb_cursor_close' rdMem
    return rcDiffs

readDataDeps :: VSpace -> Address -> MDB_val -> IO [Address]
readDataDeps vc addr vData = _vget vgetInit state0 >>= toDeps where
    toDeps (VGetR () sf) = return (vget_children sf)
    toDeps (VGetE eMsg) = addrBug addr $ "malformed data\n" ++ indent "  " eMsg
    state0 = VGetS
        { vget_children = []
        , vget_target = mv_data vData
        , vget_limit = mv_data vData `plusPtr` fromIntegral (mv_size vData)
        , vget_space = vc
        }

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
            addrBug maxOldDep ("in allocator's space (>= " ++ show allocStart ++ ")")

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
    let appendEntry  = compileWriteFlags [MDB_APPEND]
    let updateKey    = compileWriteFlags []
    let updateCursor = compileWriteFlags [MDB_CURRENT]
    let deleteCursor = compileWriteFlags []
    let newEphemeron addr = void $ do -- just write a zero (no persistent references)
            poke pAddr addr -- prepare vAddr and pvAddr
            mdb_cursor_put' appendEntry wc0 vAddr vZero
    let newAllocation addr rc =
            if (0 == rc) then newEphemeron addr else void $ do
            unless (rc > 0) (addrBug addr "allocated with negative refct")
            vRefct <- writeRefctBytes pRefctBuff rc
            poke pAddr addr -- prepare vAddr and pvAddr
            mdb_cursor_put' appendEntry wrc vAddr vRefct
    let moveFromZero addr rc = void $ do
            unless (rc > 0) (addrBug addr "has negative refct")
            bZeroFound <- mdb_cursor_get' MDB_SET wc0 pvAddr pvData
            unless bZeroFound (addrBug addr "has undefined refct")
            mdb_cursor_del' deleteCursor wc0 -- delete old zero!
            vRefct <- writeRefctBytes pRefctBuff rc
            mdb_cursor_put' updateKey wrc vAddr vRefct
    let updateRefct (addr,rcDiff) = 
            if (addr >= allocInit) then newAllocation addr rcDiff else 
            if (rcDiff /= 0) then addrBug addr "invalid refct update" else
            poke pAddr addr >> -- prepare vAddr and pvAddr
            mdb_cursor_get' MDB_SET wrc pvAddr pvData >>= \ bOldRefct ->
            if (not bOldRefct) then moveFromZero addr rcDiff else
            peek pvData >>= readRefctBytes >>= \ rcOld -> 
            let rc = rcOld + rcDiff in
            if (rc < 0) then addrBug addr "updated to negative refct" else
            if (0 == rc) 
                then do mdb_cursor_del' deleteCursor wrc
                        void $ mdb_cursor_put' updateKey wc0 vAddr vZero
                else do vRefct <- writeRefctBytes pRefctBuff rc
                        void $ mdb_cursor_put' updateCursor wrc vAddr vRefct

    -- process every reference count update
    mapM_ updateRefct (Map.toAscList rcDiffMap)
    mdb_cursor_close' wc0
    mdb_cursor_close' wrc

-- paranoid checks for bugs that should be impossible
addrBug :: Address -> String -> IO a
addrBug addr msg = fail $ "VCache bug: address " ++ show addr ++ " " ++ msg

-- simple variable-width encoding for reference counts. Encoding is 
-- base256 then adding 1. (The lead byte should always be non-zero,
-- but that isn't checked here.)
readRefctBytes :: MDB_val -> IO Refct
readRefctBytes v = rd (mv_data v) (mv_size v) 0 where
    rd _ 0 rc = return (rc + 1)
    rd p n rc = do
        w8 <- peek p
        let rc' = (rc `shiftL` 8) .|. (fromIntegral w8) 
        rd (p `plusPtr` 1) (n - 1) rc'

-- compute list of bytes for a big-endian encoding. 
-- Reference count must be positive!
toRefctBytes :: Refct -> [Word8]
toRefctBytes = rcb [] . subtract 1 . assertPositive where
    rcb l 0 = l
    rcb l rc = rcb (fromIntegral rc : l) (rc `shiftR` 8)
    assertPositive n = assert (n > 0) n

writeRefctBytes :: Ptr Word8 -> Refct -> IO MDB_val
writeRefctBytes p0 = wrcb p0 0 . toRefctBytes where
    wrcb _ n [] = return $! MDB_val { mv_data = p0, mv_size = n }
    wrcb p n (b:bs) = do { poke p b ; wrcb (p `plusPtr` 1) (n + 1) bs }

-- Primary code for updating the virtual memory. This writes in two
-- phases: first, it updates existing content, then writes any new
-- content to the end of the address space. This operation does not
-- attempt to read anything.
updateVirtualMemory :: VSpace -> MDB_txn -> Address -> WriteBatch -> IO ()
updateVirtualMemory vc txn allocStart fb = 
    fail "VCache todo: update memory"


runGarbageCollector :: VSpace -> MDB_txn -> Address -> IO ()
runGarbageCollector vc txn allocInit =
    fail "TODO: garbage collection"





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
