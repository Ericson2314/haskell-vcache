
-- For VCache, I need strict bytestrings for Put. 
--
-- The binary and cereal libraries seem to have converged on use of
-- a 'Builder' for outputting streams of bytecode. However, I don't
-- need streams. I just need the final blob - hopefully, with its
-- size reduced 
-- Most binary libraries
-- To optimize Put for VCache doesn't need much. The main difference
-- is that I must build a ByteString builder.
module Database.VCache.Put
    ( 
    ) where




