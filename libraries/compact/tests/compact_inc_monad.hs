{-# LANGUAGE BangPatterns #-}

module Main where

import Control.Exception
import System.Mem

import Data.Compact.Incremental
import Data.Compact.Monad
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

makeCompact :: Compact () -> IO (Maybe (Compact (String, String, String, Int, Int)))
makeCompact = runCompactM $ do
  !v1 <- compactPut "hello"
  !v2 <- compactPut "world"
  !v3 <- return 77
  !v4 <- return 78
  let !v5 = v3+v4
  !v5' <- return v5
  let v6 = v3+v4
  !v6' <- return v6
  !v7' <- compactPut (v1 ++ v2)
  return (v1, v2, v7', v3, v6')

main = do
  maybeStr <- compactNew 4096 ()
  case maybeStr of
    Nothing -> assertFail "failed to create the compact"
    Just str -> do
      maybeStr2 <- makeCompact str
      case maybeStr2 of
        Nothing -> assertFail "failed to append to the compact"
        Just str2 -> do
          -- check the values in the compact
          assertEquals ("hello", "world", "helloworld", 77, 155)
            (compactGetRoot str2)
          performMajorGC
          -- check the values in the compact again
          assertEquals ("hello", "world", "helloworld", 77, 155)
            (compactGetRoot str2)
