
module Database.VCache.PVar
    ( PVar
    , newPVar
    , newPVarIO
    , loadRootPVar
    , loadRootPVarIO
    , readPVar
    , writePVar
    , unsafePVarAddr
    ) where

import Control.Concurrent.STM
import Database.VCache.Types
import Database.VCache.Alloc (newPVar, newPVarIO, loadRootPVar, loadRootPVarIO)

-- | Read a PVar as part of a transaction.
readPVar :: PVar a -> VTx a
readPVar pvar = 
    getVTxSpace >>= \ space ->
    if (space /= pvar_space pvar) then fail "readPVar: wrong VSpace" else
    liftSTM $ readTVar (pvar_data pvar) >>= \ rdv ->
              case rdv of { (RDV v) -> return v }

-- | Write a PVar as part of a transaction.
writePVar :: PVar a -> a -> VTx ()
writePVar pvar v = 
    getVTxSpace >>= \ space ->
    if (space /= pvar_space pvar) then fail "writePVar: wrong VSpace" else
    markForWrite pvar >>
    liftSTM (writeTVar (pvar_data pvar) (RDV v))

-- | Each PVar has a stable address in the VCache. This address will
-- be very stable, but is not deterministic and isn't really something
-- you should use as information about the PVar. Mostly, it is supported
-- here to enable hashtables or memoization tables involving PVars.
unsafePVarAddr :: PVar a -> Address
unsafePVarAddr = pvar_addr

