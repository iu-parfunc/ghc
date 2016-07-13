{-# LANGUAGE Unsafe #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE MagicHash, UnboxedTuples #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# OPTIONS_HADDOCK hide #-}

module GHC.Barrier (
        storeLoadBarrier
   ) where

import GHC.Base
import GHC.IO

-- | Issue a Store/Load barrier.  Implementation varies by computing architecture.
--   The effect is to prevent earlier store operations from being
--   reordered past later Load operations.
{-# INLINE storeLoadBarrier #-}
storeLoadBarrier :: IO ()
storeLoadBarrier =
     IO $ \s1# ->
       case storeLoadBarrier# s1# of
         s2# -> (# s2#, () #)
