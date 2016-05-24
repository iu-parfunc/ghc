{-# LANGUAGE Trustworthy #-} -- is it?
{-# LANGUAGE CPP, NoImplicitPrelude, MagicHash, UnboxedTuples #-}

module Data.IORef.IORef
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

#if !defined(__PARALLEL_HASKELL__)
        mkWeakIORef,
#endif
        ) where


import GHC.Base
-- import GHC.STRef
-- import GHC.IORef hiding (atomicModifyIORef)
-- import qualified GHC.IORef
#if !defined(__PARALLEL_HASKELL__)
import GHC.Weak
#endif
import Data.IORef.Unsafe (IORef(..), newIORef, readIORef,
                          atomicModifyIORef, atomicModifyIORef',
                          atomicWriteIORef)
import qualified Data.IORef.Unsafe as U


modifyIORef :: IORef a -> (a -> a) -> IO ()
modifyIORef a f = U.modifyIORef a f >> storeLoadBarrier
  -- atomicModifyIORef a (\x -> (f x, ()))
-- modifyIORef = U.modifyIORef
modifyIORef' :: IORef a -> (a -> a) -> IO ()
modifyIORef' a f = U.modifyIORef' a f >> storeLoadBarrier
  -- atomicModifyIORef' a (\x -> (f x, ()))
-- modifyIORef' = U.modifyIORef'
writeIORef  :: IORef a -> a -> IO ()
writeIORef a v = U.writeIORef a v >> storeLoadBarrier
-- writeIORef = U.atomicWriteIORef
{-# INLINE modifyIORef #-}
{-# INLINE modifyIORef' #-}
{-# INLINE writeIORef #-}
                   
#if !defined(__PARALLEL_HASKELL__)
mkWeakIORef :: IORef a -> IO () -> IO (Weak (IORef a))
mkWeakIORef = U.mkWeakIORef
#endif

{-# INLINE storeLoadBarrier #-}
storeLoadBarrier :: IO ()
storeLoadBarrier = IO $ \s1# ->
  case storeLoadBarrier# s1# of
    s2# -> (# s2#, () #)

