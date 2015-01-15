{-# LANGUAGE BangPatterns #-}

module Main where

import Control.Exception
import System.Mem

import Data.Compact.Incremental
import Control.DeepSeq

import Control.Monad.Trans.Class
import Control.Monad.Trans.Maybe

assertFail :: String -> IO ()
assertFail msg = throwIO $ AssertionFailed msg

assertEquals :: (Eq a, Show a) => a -> a -> IO ()
assertEquals expected actual =
  if expected == actual then return ()
  else assertFail $ "expected " ++ (show expected)
       ++ ", got " ++ (show actual)

makeAppender :: (a -> IO (Maybe (Compact a))) -> a -> MaybeT IO a
makeAppender app val = do
  str <- MaybeT $ app val
  return $ compactGetRoot str

test :: Compact () -> IO (Maybe (String, String, String, Int, Int))
test str = runMaybeT $ do
  let appendOne = makeAppender $ compactAppendOne str
      appendRec = makeAppender $ compactAppendRecursively str
      appendEval = makeAppender $ compactAppendEvaled str

  !v1 <- appendRec "hello"
  !v2 <- appendRec "world"
  !v3 <- appendOne 77
  !v4 <- appendOne 78
  let !v5 = v3+v4
  !v5' <- appendOne v5

  -- again but no bang patterns on the value
  -- (bang patterns on the return value of append* are
  -- necessary, or we get a thunk that calls compactGetRoot!)
  let v6 = v3+v4
  !v6' <- appendOne v6

  -- complex stuff, we need a deepseq
  -- it doesn't matter that values are in the compact
  -- already because we need to recreate the data
  -- structure entirely
  let v7 = v1 ++ v2
  !v7' <- appendEval (force v7)

  -- apparently complex, but it's all made of data
  -- previously in the compact so we can send it
  -- with appendOne
  -- do not use v6 or v5 here, it will assert or crash!
  appendOne (v1, v2, v7', v3, v6')

main = do
  maybeStr <- compactNew 4096 ()
  case maybeStr of
    Nothing -> assertFail "failed to create the compact"
    Just str -> do
      maybeStr2 <- test str
      case maybeStr2 of
        Nothing -> assertFail "failed to append to the compact"
        Just root2 -> do
          -- check the values in the compact
          assertEquals ("hello", "world", "helloworld", 77, 155) root2
          performMajorGC
          -- check the values in the compact again
          assertEquals ("hello", "world", "helloworld", 77, 155) root2
