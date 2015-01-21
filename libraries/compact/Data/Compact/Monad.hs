{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Data.Compact.Monad
-- Copyright   :  (c) The University of Glasgow 2001-2009
--                (c) Giovanni Campagna <gcampagn@cs.stanford.edu> 2015
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
-- This module provides a Monad for executing code in the context of a
-- compact, so that all values threaded through the monad are stored in
-- the compact.
--
-- /Since: 1.0.0/

module Data.Compact.Monad (
  CompactM,
  runCompactM,
  compactPut,
  ) where

import Control.Monad
import Data.Compact.Incremental

newtype CompactM b a = CompactM {
  runCompactM :: Compact b -> IO (Maybe (Compact a)) }

instance Monad (CompactM b) where
  return v = CompactM $ \str -> compactAppendOne str v

  m >>= f = CompactM $ \str -> do
    mstr' <- runCompactM m str
    case mstr' of
      Nothing -> return Nothing
      Just str' -> let v = compactGetRoot str'
                       -- you may think that this is str', because
                       -- CompactM behaves like a state monad, where
                       -- the compact is threaded down
                       -- but if you do so you get type errors because
                       -- runCompactM here wants a Compact b, where b
                       -- is rigidly bound by the instance declaration,
                       -- while str' is a Compact a, where a is rigidly
                       -- bound by the signature of >>=
                       -- (this is why state monads don't let you change
                       -- the type of the state!)
                       -- but in this specific case it doesn't matter,
                       -- because both str and str' share the underlying
                       -- buffer, which is what we need to thread the
                       -- state down for
                   in v `seq` runCompactM (f v) str

instance Applicative (CompactM b) where
  pure = return
  (<*>) = ap

instance Functor (CompactM b) where
  fmap = liftM

compactPut :: Compactable a => a -> CompactM b a
compactPut v = CompactM $ \str -> compactAppendRecursively str v

