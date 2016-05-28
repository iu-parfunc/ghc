{-# LANGUAGE Unsafe #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE MagicHash, UnboxedTuples #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# OPTIONS_HADDOCK hide #-}

module GHC.Barrier (
        _storeLoadBarrier
   ) where

import GHC.Base
import GHC.IO

{-# INLINE _storeLoadBarrier #-}
_storeLoadBarrier :: IO ()
_storeLoadBarrier = IO $ \s1# ->
  case storeLoadBarrier# s1# of
    s2# -> (# s2#, () #)
