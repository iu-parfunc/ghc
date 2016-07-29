{-# LANGUAGE Unsafe #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE MagicHash, UnboxedTuples #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# OPTIONS_HADDOCK hide #-}

module GHC.Barrier (
        storeLoadBarrier
      , barrierCounts
   ) where

import GHC.Base
import GHC.IORef
import GHC.IO
import System.IO.Unsafe (unsafePerformIO)
import GHC.Word
import GHC.Show (show)
import GHC.Num ((+))
import GHC.Foreign (withCString)
import GHC.IO.Encoding.UTF8 (utf8)
import GHC.Ptr (Ptr(..))

{-# NOINLINE barrierCounts #-}
barrierCounts :: IORef Word64
barrierCounts = unsafePerformIO (newIORef 0)

-- | Issue a Store/Load barrier.  Implementation varies by computing architecture.
--   The effect is to prevent earlier store operations from being
--   reordered past later Load operations.
{-# INLINE storeLoadBarrier #-}
storeLoadBarrier :: IO ()
storeLoadBarrier =
  do n <- atomicModifyIORef barrierCounts (\n -> (n+1,n+1))
     traceEventIO ("StoreLoadBarriers: "++show n)
     IO $ \s1# ->
       case storeLoadBarrier# s1# of
         s2# -> (# s2#, () #)

-- Duplicated from Debug.Trace to avoid cycles:
traceEventIO :: String -> IO ()
traceEventIO msg =
    withCString utf8 msg $ \(Ptr p) -> IO $ \s ->
      case traceEvent# p s of s' -> (# s', () #)
