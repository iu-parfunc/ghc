
{-# LANGUAGE MagicHash #-}

module Main (main) where

import GHC.Base
import GHC.Integer.GMP2.Internals

main :: IO ()
main = (encodeDouble 0 :: Double) `seq` return ()

{-# INLINE encodeDouble #-}
encodeDouble :: Integer -> Double
encodeDouble (SI# _)  = D# 3.0##
encodeDouble (Jp# _)  = D# 4.0##
encodeDouble (Jn# _)  = D# 5.0##
