
-- | read-write lock specialized for using LMDB with MDB_NOLOCK option
--
module Database.VCache.RWLock
    ( RWLock
    , newRWLock
    , withRWLock
    , withRdOnlyLock
    ) where

import Control.Monad
import Control.Exception
import Control.Concurrent.MVar
import Data.IORef
import Data.IntSet (IntSet)
import qualified Data.IntSet as IntSet

--
-- NOTE: I assumed earlier, based on discussion with LMDB developers on 
-- the OpenLDAP list, that LMDB supports triple buffering (preserving 
-- two snapshots). This turns out to be wrong, so I need to tweak it.
-- However, I'm looking into tweaking LMDB fit the assumptions of the
-- developers. I can later modify this to be version-adaptive.
bUseSingleBuffer :: Bool
bUseSingleBuffer = True

-- | RWLock
--
-- VCache uses LMDB with the MDB_NOLOCK option, mostly because I don't
-- want to deal with the whole issue of OS bound threads or a limit on
-- number of concurrent readers. Without locks, we essentially have one
-- valid snapshot. The writer can begin dismantling earlier snapshots
-- as needed to allocate pages. 
--
-- RWLock essentially enforces this sort of frame-buffer concurrency.
data RWLock = RWLock 
    { rwlock_frames :: !(MVar FB)
    , rwlock_writer :: !(MVar ())  -- enforce single writer
    }

data FB = FB !F !F
type F = IORef Frame
data Frame = Frame 
    { frame_reader_next :: {-# UNPACK #-} !Int
    , frame_readers     :: !IntSet
    , frame_onClear     :: ![IO ()] -- actions to perform 
    }
frame0 :: Frame
frame0 = Frame 1 IntSet.empty []

newRWLock :: IO RWLock
newRWLock = liftM2 RWLock (newMVar =<< newF2) newEmptyMVar where

newF2 :: IO FB
newF2 = liftM2 FB newF newF 

newF :: IO F
newF = newIORef frame0

withWriterMutex :: RWLock -> IO a -> IO a
withWriterMutex l = bracket_ getLock dropLock where
    getLock = putMVar (rwlock_writer l) ()
    dropLock = takeMVar (rwlock_writer l)
{-# INLINE withWriterMutex #-}

-- | Grab the current read-write lock for the duration of
-- an underlying action. This may wait on older readers. 
withRWLock :: RWLock -> IO a -> IO a
withRWLock l action = withWriterMutex l $ do
    oldFrame <- rotateReaderFrames l 
    mvWait <- newEmptyMVar
    onFrameCleared oldFrame (putMVar mvWait ())
    takeMVar mvWait
    action

-- rotate a fresh reader frame, and grab the oldest.
-- Thus should only be performed while holding the writer lock.
rotateReaderFrames :: RWLock -> IO F

rotateReaderFrames l | bUseSingleBuffer = mask_ $ do
    let var = rwlock_frames l
    f0 <- newF
    (FB f1 fUnused) <- takeMVar var
    putMVar var (FB f0 fUnused)
    return f1

rotateReaderFrames l = mask_ $ do
    let var = rwlock_frames l
    f0 <- newF
    (FB f1 f2) <- takeMVar var
    putMVar var (FB f0 f1)
    return f2

-- 
-- Unfortunately, each writer fr
--   a writer at frame N can tolerate readers at frame N-1 but not
--   readers at frame N-2. 

--
-- NOTE: Each of these 'frames' actually contains content for
-- TWO LMDB frames, since the alignment between LMDB transactions 
-- and VCache locks isn't exact. 
--
-- Each writer will rotate the frames just once. 
--
--     (f1,f2) → (f0,f1) returning f2
--
-- Writer is working on frame N.
--
-- f0 will have readers for frame N-1 and later for N.
-- f1 will have readers for frame N-2 and some for N-1.
-- f2 will have readers for frame N-3 and some for N-2.
--
-- After writer commits frame N, writer soon begins next frame. So,
-- there isn't much of a window for frame f0 to receive readers on
-- frame N. However, since this RWLock is not precisely aligned with
-- writer transactions, we should assume some overlap.
--
-- According to discussion on the OpenLDAP list, LMDB keeps two frames
-- valid at all times. The writer for frame N, thus, cannot invalidate
-- the content for frames N-1 or N-2, but can overwrite content from
-- frame N-3. 
--
-- After rotate, f0 begins receiving readers for the current frame,
-- and will eventually receive content for the next frame after the
-- writer commits. Meanwhile, f1 has readers for the current frame
-- and the previous one, and f2 has readers for a previous safe frame 
-- and what will become an unsafe frame after the writer reuses some
-- 'free' pages.
--
-- If writes are sparse and fast, this reduces to double buffering.
-- But if writes are continuous, readers are effectively triple 
-- buffered.
--


-- perform some action when a frame is cleared 
-- performs immediately, if possible.
onFrameCleared :: F -> IO () -> IO ()
onFrameCleared f action = atomicModifyIORef f addAction >>= id where
    addAction frame =
        let bAlreadyClear = IntSet.null (frame_readers frame) in
        if bAlreadyClear then (frame0,action) else
        let onClear' = action : frame_onClear frame in
        let frame' = frame { frame_onClear = onClear' } in
        (frame', return ())

-- | Grab a read-only lock for the duration of some IO action. 
--
-- Readers never need to wait on the writer.
withRdOnlyLock :: RWLock -> IO a -> IO a
withRdOnlyLock l = bracket (newReader l) releaseReader . const

newtype Reader = Reader { releaseReader :: IO () }

-- obtains a reader handle; returns function to release reader.
newReader :: RWLock -> IO Reader
newReader l = mask_ $ do
    let var = rwlock_frames l
    fb@(FB f _) <- takeMVar var
    r <- atomicModifyIORef f addReader
    putMVar var fb
    return (Reader (delReader f r))

addReader :: Frame -> (Frame, Int)
addReader f =
    let r     = frame_reader_next f in
    let rdrs' = IntSet.insert r (frame_readers f) in
    let f'    = f { frame_reader_next = (r + 1)
                  , frame_readers = rdrs' } in
    (f', r)

delReader :: F -> Int -> IO ()
delReader f r = atomicModifyIORef f del >>= id where
    del frm =
        let rdrs' = IntSet.delete r (frame_readers frm) in
        if IntSet.null rdrs' then (frame0, sequence_ (frame_onClear frm)) else
        let frm' = frm { frame_readers = rdrs' } in
        (frm', return ()) 
