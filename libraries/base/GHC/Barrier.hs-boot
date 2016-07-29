{-# LANGUAGE NoImplicitPrelude #-}
module GHC.Barrier (storeLoadBarrier) where

import GHC.IO (IO)

storeLoadBarrier :: IO ()
