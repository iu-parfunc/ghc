{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Data.Struct
-- Copyright   :  (c) The University of Glasgow 2001-2009
--                (c) Giovanni Campagna <gcampagn@cs.stanford.edu> 2014
-- License     :  BSD-style (see the file LICENSE)
--
-- Maintainer  :  libraries@haskell.org
-- Stability   :  unstable
-- Portability :  non-portable (GHC Extensions)
--
-- This module provides a data structure, called a Struct, for holding
-- a set of fully evaluated Haskell values in a consecutive block of
-- memory.
--
-- As the data fully evaluated and pure (thus immutable), it maintains
-- the invariant that no memory reference exist from objects inside the
-- Struct to objects outside, thus allowing very fast garbage collection
-- (at the expense of increased memory usage, because the entire set of
-- object is kept alive if any object is alive).
--
-- /Since: 1.0.0/
module Data.Struct (
  Struct,
  structGetRoot,

  structNew,
  structAppend,
  structResize,
  ) where

-- Write down all GHC.Prim deps explicitly to keep them at minimum
import GHC.Prim (Struct#,
                 structNew#,
                 structAppend#,
                 structResize#,
                 Addr#,
                 nullAddr#,
                 eqAddr#,
                 addrToAny#,
                 State#,
                 RealWorld,
                 )
-- We need to import Word from GHC.Types to see the representation
-- and to able to access the Word# to pass down the primops
import GHC.Types (IO(..), Word(..), isTrue#)

import Control.DeepSeq (NFData, force)

data Struct a = Struct Struct# a

-- | 'structGetRoot': retrieve the object that was stored in a Struct
structGetRoot :: Struct a -> a
structGetRoot (Struct _ obj) = obj

structGetBuffer :: Struct a -> Struct#
structGetBuffer (Struct buffer _) = buffer

addrIsNull :: Addr# -> Bool
addrIsNull addr = isTrue# (nullAddr# `eqAddr#` addr)

maybeMakeStruct :: Struct# -> Addr# -> Maybe (Struct a)
maybeMakeStruct _ rootAddr | addrIsNull rootAddr = Nothing
maybeMakeStruct buffer rootAddr = Just $ makeStruct buffer rootAddr

makeStruct :: Struct# -> Addr# -> Struct a
makeStruct buffer rootAddr =
  case addrToAny# rootAddr of
    (# root #) -> Struct buffer root

structAppendInternal :: NFData a => Struct# -> a -> State# RealWorld ->
                        (# State# RealWorld, Maybe (Struct a) #)
structAppendInternal buffer root s =
  case structAppend# buffer (force root) s of
    (# s', rootAddr #) -> (# s', maybeMakeStruct buffer rootAddr #)

structAppend :: NFData a => Struct b -> a -> IO (Maybe (Struct a))
structAppend str root =
  IO (\s -> structAppendInternal (structGetBuffer str) root s)

structNew :: NFData a => Word -> a -> IO (Maybe (Struct a))
structNew (W# size) root =
  IO (\s -> case structNew# size s of
         (# s', buffer #) -> structAppendInternal buffer root s' )

structResize :: Struct a -> Word -> IO (Struct a)
structResize (Struct oldBuffer oldRoot) (W# new_size) =
  IO (\s -> case structResize# oldBuffer oldRoot new_size s of
         (# s', buffer, rootAddr #) ->
           (# s', makeStruct buffer rootAddr #) )
