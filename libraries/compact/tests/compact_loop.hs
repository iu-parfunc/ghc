module Main where

import Control.Exception
import Control.DeepSeq
import System.Mem
import Text.Show

import Data.Compact

assertFail :: String -> IO ()
assertFail msg = throwIO $ AssertionFailed msg

assertEquals :: (Eq a, Show a) => a -> a -> IO ()
assertEquals expected actual =
  if expected == actual then return ()
  else assertFail $ "expected " ++ (show expected)
       ++ ", got " ++ (show actual)

data Tree = Nil | Node Tree Tree Tree

instance Eq Tree where
  Nil == Nil = True
  Node _ l1 r1 == Node _ l2 r2 = l1 == l2 && r1 == r2
  _ == _ = False

instance Show Tree where
  showsPrec _ Nil = showString "Nil"
  showsPrec _ (Node _ l r) = showString "(Node " . shows l .
                             showString " " . shows r . showString ")"

instance NFData Tree where
  rnf Nil = ()
  rnf (Node p l r) = p `seq` rnf l `seq` rnf r `seq` ()

{-# NOINLINE test #-}
test x = do
  let a = Node Nil x b
      b = Node a Nil Nil
  maybeStr <- compactNew 4096 a
  case maybeStr of
    Nothing -> assertFail "failed to create the compact"
    Just str -> do
      -- check the value in the compact
      assertEquals a (compactGetRoot str)
      performMajorGC
      -- check again the value in the compact
      assertEquals a (compactGetRoot str)
  -- now try with compactNewNoShare (should fail)
  maybeStr2 <- compactNewNoShare 4096 a
  case maybeStr2 of
    Nothing -> return ()
    Just str2 -> assertFail "unexpectedly created a compact with a loop"


main = test Nil
