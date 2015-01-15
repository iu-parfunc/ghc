{-# LANGUAGE BangPatterns #-}

module Main where

import Control.Exception
import System.Mem

import Data.Compact.Incremental

assertFail :: String -> IO ()
assertFail msg = throwIO $ AssertionFailed msg

assertEquals :: (Eq a, Show a) => a -> a -> IO ()
assertEquals expected actual =
  if expected == actual then return ()
  else assertFail $ "expected " ++ (show expected)
       ++ ", got " ++ (show actual)

main = do
  let val = ("hello", Just 42) :: (String, Maybe Int)
  maybeStr <- compactNew 4096 val
  case maybeStr of
    Nothing -> assertFail "failed to create the compact"
    Just str -> do
      let val2 = ("world", 42) :: (String, Int)
      maybeStr2 <- compactAppendRecursively str val2
      case maybeStr2 of
        Nothing -> assertFail "failed to append to the compact"
        Just str2 -> do
          -- check that values where not corrupted
          assertEquals ("hello", Just 42) val
          assertEquals ("world", 42) val2
          -- check the values in the compact
          assertEquals ("hello", Just 42) (compactGetRoot str)
          assertEquals ("world", 42) (compactGetRoot str2)

          performMajorGC

          -- same checks again
          assertEquals ("hello", Just 42) val
          assertEquals ("world", 42) val2
          -- check the values in the compact
          assertEquals ("hello", Just 42) (compactGetRoot str)
          assertEquals ("world", 42) (compactGetRoot str2)

          -- append the pair obtained from str1 and str2
          let !v1 = compactGetRoot str
              !v2 = compactGetRoot str2
          maybeStr3 <- compactAppendOne str (v1, v2)
          case maybeStr3 of
            Nothing -> assertFail "failed to append to the compact"
            Just str3 -> do
              assertEquals (("hello", Just 42),("world",42))
                (compactGetRoot str3)
