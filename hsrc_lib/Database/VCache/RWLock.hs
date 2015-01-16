
-- read-write lock specialized for using LMDB with MDB_NOLOCK option
module Database.VCache.RWLock
    ( {- RWLock
    , newRWLock
    , withRWLock
    , withRdOnlyLock -}
    ) where


-- | RWLock
--
-- VCache uses LMDB with the MDB_NOLOCK option, mostly because I don't
-- want to deal with the whole issue of OS bound threads or a limit on
-- number of concurrent readers. 
--
-- LMDB without locking is a bit tricky. The basic rule is that only one
-- writer may be active at a time, and readers must not exist for 'old'
-- transactions. LMDB keeps data from two transactions in the database.
-- 
-- Having two prior versions of the database can support up to triple 
-- buffering. That is, readers of the most recent two 'frames' may be
-- active while the writer is constructing the third frame. This 
-- becomes the foundation for this frame-based reader-writer lock.
--
-- The consequence is that new readers never wait on writers, and writers
-- only wait on *old* readers. So long as read transactions are kept to
-- a minimum (and VCache does this), writers will rarely need to wait, and
-- any waits will support a useful fairness model.
--
-- RWLock does not guard opening or closing of MDB_dbi handles. Developers
-- should have exclusive control of the database for that purpose, but it
-- is usually a one time deal at app start anyway.
-- 
