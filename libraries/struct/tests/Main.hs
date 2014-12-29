-- Code reused from http://hackage.haskell.org/package/deepseq-generics

{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TupleSections #-}

module Main (main) where

-- import Test.Framework (defaultMain, testGroup, testCase)
import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit

-- IUT
import Data.Struct

-- TODO

main :: IO ()
main = defaultMain [tests]
  where
    tests = testGroup "" []
