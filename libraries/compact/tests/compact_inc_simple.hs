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

test :: Int -> IO ()
test x = do
  let val = ("hello", 1, x, 42, Just 42) :: (String, Int, Int, Integer, Maybe Int)
  str <- compactNew 4096 val

  -- check that val is still good
  assertEquals ("hello", 1, x, 42, Just 42) val
  -- check the value in the compact
  assertEquals ("hello", 1, x, 42, Just 42) (compactGetRoot str)
  performMajorGC
  -- check again val
  assertEquals ("hello", 1, x, 42, Just 42) val
  -- check again the value in the compact
  assertEquals ("hello", 1, x, 42, Just 42) (compactGetRoot str)

main = do
  -- just in case the compiler is smart and inlines test
  -- floating out val to be a constant and screwing up our
  -- HEAP_ALLOCED() test
  test 42
  test 43

