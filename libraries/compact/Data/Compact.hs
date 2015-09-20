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
  getCompact,
  inCompact,
  isCompact,

  newCompact,
  newCompactAt,
  newCompactNoShare,
  newCompactNoShareAt,
  appendCompact,
  appendCompactNoShare,

  SerializedCompact(..),
  withCompactPtrs,
  importCompact,
  importCompactByteStrings,
  ) where

-- Write down all GHC.Prim deps explicitly to keep them at minimum
import GHC.Prim (Compact#,
                 compactNew#,
                 State#,
                 RealWorld,
                 Int#,
                 )
-- We need to import Word from GHC.Types to see the representation
-- and to able to access the Word# to pass down the primops
import GHC.Types (IO(..), Word(..))

import GHC.Ptr (Ptr(..))
import qualified Data.ByteString as BS

import Control.DeepSeq (NFData, force)

import Data.Compact.Imp(Compact(..),
                        compactGetRoot,
                        compactContains,
                        compactContainsAny,
                        compactAppendEvaledInternal,
                        SerializedCompact(..),
                        withCompactPtrsInternal,
                        compactImport,
                        compactImportByteStrings)

getCompact :: Compact a -> a
getCompact = compactGetRoot

inCompact :: Compact a -> b -> Bool
inCompact = compactContains

isCompact :: a -> Bool
isCompact = compactContainsAny

compactAppendInternal :: NFData a => Compact# -> a -> Int# -> State# RealWorld ->
                        (# State# RealWorld, Compact a #)
compactAppendInternal buffer root share s =
  case force root of
    !eval -> compactAppendEvaledInternal buffer eval share s

compactAppendInternalIO :: NFData a => Int# -> Compact b -> a -> IO (Compact a)
compactAppendInternalIO share (Compact buffer _) root =
  IO (\s -> compactAppendInternal buffer root share s)

appendCompact :: NFData a => Compact b -> a -> IO (Compact a)
appendCompact = compactAppendInternalIO 1#

appendCompactNoShare :: NFData a => Compact b -> a -> IO (Compact a)
appendCompactNoShare = compactAppendInternalIO 0#

compactNewInternal :: NFData a => Int# -> Addr# -> Word -> a -> IO (Compact a)
compactNewInternal share addr_hint (W# size) root =
  IO (\s -> case compactNew# size addr_hint s of
         (# s', buffer #) -> compactAppendInternal buffer root share s' )

newCompact :: NFData a => Word -> a -> IO (Compact a)
newCompact = compactNewInternal 1# nullAddr#

newCompactNoShare :: NFData a => Word -> a -> IO (Compact a)
newCompactNoShare = compactNewInternal 0# nullAddr#

newCompactAt :: NFData a => Word -> Ptr b -> a -> IO (Compact a)
newCompactAt size (Ptr addr_hint) = compactNewInternal 1# addr_hint size

newCompactNoShareAt :: NFData a => Word -> Ptr b -> a -> IO (Compact a)
newCompactNoShareAt size (Ptr addr_hint) = compactNewInternal 0# addr_hint size

withCompactPtrs :: NFData c => Compact a -> (SerializedCompact a -> IO c) -> IO c
withCompactPtrs = withCompactPtrsInternal

importCompact :: SerializedCompact a -> (Ptr b -> Word -> IO ()) -> IO (Maybe (Compact a))
importCompact = compactImport

importCompactByteStrings :: SerializedCompact a -> [BS.ByteString] -> IO (Maybe (Compact a))
importCompactByteStrings = compactImportByteStrings
