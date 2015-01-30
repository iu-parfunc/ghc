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

main = do
  -- the take thing is to force creating a thunk even though
  -- the value is at (almost) global scope, which will make
  -- sure the value is not static

  -- v2 takes 512 cons cells (3 words each, 4 when profiling),
  -- for a total of 2048 words. It will not fit in the compact
  -- initially sized (1 block, 512 words).
  -- (v1 OTOH will because of block alignment)
  -- After resizing, we reserve 2560 (5 words per cell) just to
  -- be safe. Note that we assume the 7 is not copied around,
  -- rather all cons cells point to the same object (which
  -- should be a static INTLIKE, but that's too much to ask
  -- if the optimizer is off)
  let v1 = take 2 [1..] :: [Int]
      v2 = replicate 512 7 :: [Int]
  str1 <- compactNew 1 v1

  compactResize str1 (2560*8)
  str2 <- compactAppend str1 v2

  assertEquals v1 (compactGetRoot str1)
  assertEquals v2 (compactGetRoot str2)

