{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Data.Compact
-- Copyright   :  (c) The University of Glasgow 2001-2009
--                (c) Giovanni Campagna <gcampagn@cs.stanford.edu> 2014
-- License     :  BSD-style (see the file LICENSE)
--
-- Maintainer  :  libraries@haskell.org
-- Stability   :  unstable
-- Portability :  non-portable (GHC Extensions)
--
-- This module provides a data structure, called a Compact, for holding
-- a set of fully evaluated Haskell values in a consecutive block of
-- memory.
--
-- As the data fully evaluated and pure (thus immutable), it maintains
-- the invariant that no memory reference exist from objects inside the
-- Compact to objects outside, thus allowing very fast garbage collection
-- (at the expense of increased memory usage, because the entire set of
-- object is kept alive if any object is alive).
--
-- /Since: 1.0.0/
module Data.Compact (
  Compact,
  compactGetRoot,

  compactNew,
  compactNewAt,
  compactNewNoShare,
  compactNewNoShareAt,
  compactNewSmall,
  compactAppend,
  compactAppendSmall,
  compactAppendNoShare,
  compactResize,

  SerializedCompact(..),
  withCompactPtrs,
  compactImport,
  compactImportTrusted,
  compactImportByteStrings,
  compactImportByteStringsTrusted,

  compactInitForSymbols,
  compactBuildSymbolTable,
  ) where

-- Write down all GHC.Prim deps explicitly to keep them at minimum
import GHC.Prim (Compact#,
                 compactNew#,
                 State#,
                 RealWorld,
                 Int#,
                 Addr#,
                 nullAddr#,
                 )
-- We need to import Word from GHC.Types to see the representation
-- and to able to access the Word# to pass down the primops
import GHC.Types (IO(..), Word(..))

import GHC.Ptr (Ptr(..))

import Control.DeepSeq (NFData, force)

import Data.Compact.Imp(Compact(..),
                        compactGetRoot,
                        compactResize,
                        compactNewSmall,
                        compactAppendEvaledInternal,
                        SerializedCompact(..),
                        withCompactPtrsInternal,
                        compactImport,
                        compactImportTrusted,
                        compactImportByteStrings,
                        compactImportByteStringsTrusted,
                        compactInitForSymbols,
                        compactBuildSymbolTable)

compactAppendInternal :: NFData a => Compact# -> a -> Int# -> State# RealWorld ->
                        (# State# RealWorld, Compact a #)
compactAppendInternal buffer root share s =
  case force root of
    !eval -> compactAppendEvaledInternal buffer eval share s

compactAppendInternalIO :: NFData a => Int# -> Compact b -> a -> IO (Compact a)
compactAppendInternalIO share (LargeCompact buffer _) root =
  IO (\s -> compactAppendInternal buffer root share s)
compactAppendInternalIO share (SmallCompact _) root =
  compactNewLargeInternal share nullAddr# 4096 root

compactAppend :: NFData a => Compact b -> a -> IO (Compact a)
compactAppend = compactAppendInternalIO 1#

compactAppendNoShare :: NFData a => Compact b -> a -> IO (Compact a)
compactAppendNoShare = compactAppendInternalIO 0#

compactAppendSmall :: NFData a => Compact b -> a -> IO (Compact a)
compactAppendSmall (SmallCompact _) = compactNewSmall
compactAppendSmall str = compactAppend str

compactNewLargeInternal :: NFData a => Int# -> Addr# -> Word -> a -> IO (Compact a)
compactNewLargeInternal share addr_hint (W# size) root =
  IO (\s -> case compactNew# size addr_hint s of
         (# s', buffer #) -> compactAppendInternal buffer root share s' )

compactNew :: NFData a => Word -> a -> IO (Compact a)
compactNew = compactNewLargeInternal 1# nullAddr#

compactNewNoShare :: NFData a => Word -> a -> IO (Compact a)
compactNewNoShare = compactNewLargeInternal 0# nullAddr#

compactNewAt :: NFData a => Word -> Ptr b -> a -> IO (Compact a)
compactNewAt size (Ptr addr_hint) = compactNewLargeInternal 1# addr_hint size

compactNewNoShareAt :: NFData a => Word -> Ptr b -> a -> IO (Compact a)
compactNewNoShareAt size (Ptr addr_hint) = compactNewLargeInternal 0# addr_hint size

compactMakeLarge :: NFData a => Compact a -> IO (Compact a)
compactMakeLarge c@(LargeCompact _ _) = return c
compactMakeLarge (SmallCompact root) = do
  compactNewLargeInternal 0# nullAddr# 4096 root

withCompactPtrs :: (NFData a, NFData c) => Compact a -> (SerializedCompact a -> IO c) -> IO c
withCompactPtrs str func = do
  largeStr <- compactMakeLarge str
  withCompactPtrsInternal largeStr func
