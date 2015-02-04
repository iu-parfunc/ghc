module Main where

import Control.Exception
import System.Mem

import Data.Compact

assertFail :: String -> IO ()
assertFail msg = throwIO $ AssertionFailed msg

assertEquals :: (Eq a, Show a) => a -> a -> IO ()
assertEquals expected actual =
  if expected == actual then return ()
  else assertFail $ "expected " ++ (show expected)
       ++ ", got " ++ (show actual)

testExplicit = do
  let val = ("hello", 1, 42, 42, Just 42) :: (String, Int, Int, Integer, Maybe Int)
  str <- compactNew 4096 val

  compactBuildSymbolTable str

  assertEquals ("hello", 1, 42, 42, Just 42) (compactGetRoot str)
  performMajorGC
  assertEquals ("hello", 1, 42, 42, Just 42) (compactGetRoot str)

testImplicit = do
  let val = ("hello", 1, 42, 42, Just 42) :: (String, Int, Int, Integer, Maybe Int)
  str <- compactNew 4096 val

  withCompactPtrs str $ \(SerializedCompact blocks _) -> do
    assertEquals 1 (length blocks)

  compactInitForSymbols str

  withCompactPtrs str $ \(SerializedCompact blocks _) -> do
    assertEquals 2 (length blocks)

  assertEquals ("hello", 1, 42, 42, Just 42) (compactGetRoot str)
  performMajorGC
  assertEquals ("hello", 1, 42, 42, Just 42) (compactGetRoot str)

main = do
  testExplicit
  testImplicit

