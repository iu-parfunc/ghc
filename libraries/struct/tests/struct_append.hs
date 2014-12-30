module Main where

import Control.Exception
import System.Mem

import Data.Struct

assertFail :: String -> IO ()
assertFail msg = throwIO $ AssertionFailed msg

assertEquals :: (Eq a, Show a) => a -> a -> IO ()
assertEquals expected actual =
  if expected == actual then return ()
  else assertFail $ "expected " ++ (show expected)
       ++ ", got " ++ (show actual)

main = do
  let val = ("hello", Just 42) :: (String, Maybe Int)
  maybeStr <- structNew 4096 val
  case maybeStr of
    Nothing -> assertFail "failed to create the struct"
    Just str -> do
      let val2 = ("world", 42) :: (String, Int)
      maybeStr2 <- structAppend str val2
      case maybeStr2 of
        Nothing -> assertFail "failed to append to the struct"
        Just str2 -> do
          -- check that values where not corrupted
          assertEquals ("hello", Just 42) val
          assertEquals ("world", 42) val2
          -- check the values in the struct
          assertEquals ("hello", Just 42) (structGetRoot str)
          assertEquals ("world", 42) (structGetRoot str2)

          performMajorGC

          -- same checks again
          assertEquals ("hello", Just 42) val
          assertEquals ("world", 42) val2
          -- check the values in the struct
          assertEquals ("hello", Just 42) (structGetRoot str)
          assertEquals ("world", 42) (structGetRoot str2)
