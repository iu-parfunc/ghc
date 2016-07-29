{-# LANGUAGE NoImplicitPrelude #-}
module GHC.Barrier (storeLoadBarrier, barrierCounts) where

import GHC.IO (IO)
import GHC.IORef (IORef)
import GHC.Word (Word64)

barrierCounts :: IORef Word64
storeLoadBarrier :: IO ()
