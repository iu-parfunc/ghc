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
  compactNewNoShare,
  compactAppend,
  compactAppendNoShare,
  compactResize,
  ) where

-- Write down all GHC.Prim deps explicitly to keep them at minimum
import GHC.Prim (Compact#,
                 compactNew#,
                 compactAppend#,
                 compactResize#,
                 Addr#,
                 nullAddr#,
                 eqAddr#,
                 addrToAny#,
                 State#,
                 RealWorld,
                 Int#,
                 )
-- We need to import Word from GHC.Types to see the representation
-- and to able to access the Word# to pass down the primops
import GHC.Types (IO(..), Word(..), isTrue#)

import Control.DeepSeq (NFData, force)

data Compact a = Compact Compact# a

-- | 'compactGetRoot': retrieve the object that was stored in a Compact
compactGetRoot :: Compact a -> a
compactGetRoot (Compact _ obj) = obj

compactGetBuffer :: Compact a -> Compact#
compactGetBuffer (Compact buffer _) = buffer

addrIsNull :: Addr# -> Bool
addrIsNull addr = isTrue# (nullAddr# `eqAddr#` addr)

maybeMakeCompact :: Compact# -> Addr# -> Maybe (Compact a)
maybeMakeCompact _ rootAddr | addrIsNull rootAddr = Nothing
maybeMakeCompact buffer rootAddr = Just $ makeCompact buffer rootAddr

makeCompact :: Compact# -> Addr# -> Compact a
makeCompact buffer rootAddr =
  case addrToAny# rootAddr of
    (# root #) -> Compact buffer root

compactAppendInternal :: NFData a => Compact# -> a -> Int# -> State# RealWorld ->
                        (# State# RealWorld, Maybe (Compact a) #)
compactAppendInternal buffer root share s =
  case force root of
    !eval -> case compactAppend# buffer eval share s of
      (# s', rootAddr #) -> (# s', maybeMakeCompact buffer rootAddr #)

compactAppendInternalIO :: NFData a => Int# -> Compact b -> a -> IO (Maybe (Compact a))
compactAppendInternalIO share str root =
  IO (\s -> compactAppendInternal (compactGetBuffer str) root share s)

compactAppend :: NFData a => Compact b -> a -> IO (Maybe (Compact a))
compactAppend = compactAppendInternalIO 1#

compactAppendNoShare :: NFData a => Compact b -> a -> IO (Maybe (Compact a))
compactAppendNoShare = compactAppendInternalIO 0#

compactNewInternal :: NFData a => Int# -> Word -> a -> IO (Maybe (Compact a))
compactNewInternal share (W# size) root =
  IO (\s -> case compactNew# size s of
         (# s', buffer #) -> compactAppendInternal buffer root share s' )

compactNew :: NFData a => Word -> a -> IO (Maybe (Compact a))
compactNew = compactNewInternal 1#

compactNewNoShare :: NFData a => Word -> a -> IO (Maybe (Compact a))
compactNewNoShare = compactNewInternal 0#

compactResize :: Compact a -> Word -> IO (Compact a)
compactResize (Compact oldBuffer oldRoot) (W# new_size) =
  IO (\s -> case compactResize# oldBuffer oldRoot new_size s of
         (# s', buffer, rootAddr #) ->
           (# s', makeCompact buffer rootAddr #) )
