
module Database.VCache.PVar
    ( PVar
    , newPVar
    , newPVarIO
    , loadRootPVar
    , loadRootPVarIO
    , readPVar
    , readPVarIO
    , writePVar
    , modifyPVar
    , modifyPVar'
    , swapPVar
    , unsafePVarAddr
    , unsafePVarRefct
    , pvar_space
    ) where

import Control.Monad
import Control.Concurrent.STM
import Database.VCache.Types
import Database.VCache.Alloc (newPVar, newPVarIO, loadRootPVar, loadRootPVarIO)
import Database.VCache.Read (readRefctIO)

-- | Read a PVar as part of a transaction.
readPVar :: PVar a -> VTx a
readPVar pvar = 
    getVTxSpace >>= \ space ->
    if (space /= pvar_space pvar) then fail eBadSpace else
    liftSTM $ liftM unRDV $ readTVar (pvar_data pvar)
{-# INLINABLE readPVar #-}

-- | Read a PVar in the IO monad. 
--
-- This is equivalent to:
--
-- > \ pv -> runVTx (pvar_space pv) (readPVar pv)
--
-- But the implementation is simply readTVarIO on the underlying
-- TVar. This operation can be relatively efficient compared to
-- performing a full transaction.
readPVarIO :: PVar a -> IO a
readPVarIO = liftM unRDV . readTVarIO . pvar_data
{-# INLINE readPVarIO #-}

unRDV :: RDV a -> a
unRDV (RDV a) = a
{-# INLINABLE unRDV #-}

eBadSpace :: String
eBadSpace = "VTx: mismatch between VTx VSpace and PVar VSpace"

-- | Write a PVar as part of a transaction.
writePVar :: PVar a -> a -> VTx ()
writePVar pvar v = 
    getVTxSpace >>= \ space ->
    if (space /= pvar_space pvar) then fail eBadSpace else
    markForWrite pvar v >>
    liftSTM (writeTVar (pvar_data pvar) (RDV v))
{-# INLINABLE writePVar #-}


-- | Modify a PVar. 
modifyPVar :: PVar a -> (a -> a) -> VTx ()
modifyPVar var f = do
    x <- readPVar var
    writePVar var (f x)
{-# INLINE modifyPVar #-}

-- | Modify a PVar, strictly.
modifyPVar' :: PVar a -> (a -> a) -> VTx ()
modifyPVar' var f = do
    x <- readPVar var
    writePVar var $! f x
{-# INLINE modifyPVar' #-}

-- | Swap contents of a PVar for a new value.
swapPVar :: PVar a -> a -> VTx a
swapPVar var new = do
    old <- readPVar var 
    writePVar var new
    return old
{-# INLINE swapPVar #-}

-- | Each PVar has a stable address in the VCache. This address will
-- be very stable, but is not deterministic and isn't really something
-- you should treat as meaningful information about the PVar. Mostly, 
-- this function exists to support hashtables or memoization with
-- PVar keys.
--
-- The Show instance for PVars will also show the address.
unsafePVarAddr :: PVar a -> Address
unsafePVarAddr = pvar_addr
{-# INLINE unsafePVarAddr #-}

-- | This function allows developers to access the reference count 
-- for the PVar that is currently recorded in the database. This may
-- be useful for heuristic purposes. However, caveats are needed:
--
-- First, because the VCache writer operates in a background thread,
-- the reference count returned here may be slightly out of date.
--
-- Second, it is possible that VCache will eventually use some other
-- form of garbage collection than reference counting. This function
-- should be considered an unstable element of the API.
--
-- Root PVars start with one root reference.
unsafePVarRefct :: PVar a -> IO Int
unsafePVarRefct var = readRefctIO (pvar_space var) (pvar_addr var)

