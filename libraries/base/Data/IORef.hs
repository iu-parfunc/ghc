{-# LANGUAGE Trustworthy #-}
{-# LANGUAGE CPP, NoImplicitPrelude, MagicHash, UnboxedTuples #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Data.IORef
-- Copyright   :  (c) The University of Glasgow 2001
-- License     :  BSD-style (see the file libraries/base/LICENSE)
-- 
-- Maintainer  :  libraries@haskell.org
-- Stability   :  experimental
-- Portability :  portable
--
-- Mutable references in the IO monad.
--
-----------------------------------------------------------------------------

module Data.IORef
    (
        -- * IORefs
        IORef,                -- abstract, instance of: Eq, Typeable
        newIORef,
        readIORef,
        writeIORef,
        modifyIORef,
        modifyIORef',
        atomicModifyIORef,
        atomicModifyIORef',
        atomicWriteIORef,

-- #if !defined(__PARALLEL_HASKELL__)
--         mkWeakIORef,
-- #endif
        ) where


-- import GHC.Base
-- import GHC.STRef
-- import GHC.IORef hiding (atomicModifyIORef)
-- import qualified GHC.IORef
-- #if !defined(__PARALLEL_HASKELL__)
-- import GHC.Weak
-- #endif
import Data.IORef.Unsafe (IORef(..), newIORef, readIORef,
                          atomicModifyIORef, atomicModifyIORef',
                          atomicWriteIORef)
import qualified Data.IORef.Unsafe as U


-- Are we going to provide alternate, safer implementations of these?
modifyIORef :: IORef a -> (a -> a) -> IO ()
modifyIORef a f = do
  atomicModifyIORef a (\x -> (f x, ()))
  return ()
-- modifyIORef = U.modifyIORef
modifyIORef' :: IORef a -> (a -> a) -> IO ()
modifyIORef' a f = do
  atomicModifyIORef' a (\x -> (f x, ()))
  return ()                
-- modifyIORef' = U.modifyIORef'
writeIORef  :: IORef a -> a -> IO ()
writeIORef = U.atomicWriteIORef
{-# INLINE modifyIORef #-}
{-# INLINE modifyIORef' #-}
{-# INLINE writeIORef #-}
                   
-- #if !defined(__PARALLEL_HASKELL__)
-- mkWeakIORef :: IORef a -> IO () -> IO (Weak (IORef a))
-- mkWeakIORef = U.mkWeakIORef
-- #endif
