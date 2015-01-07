module Main where

import Control.Exception
import Control.DeepSeq
import System.Mem

import Data.Compact

assertFail :: String -> IO ()
assertFail msg = throwIO $ AssertionFailed msg

assertEquals :: (Eq a, Show a) => a -> a -> IO ()
assertEquals expected actual =
  if expected == actual then return ()
  else assertFail $ "expected " ++ (show expected)
       ++ ", got " ++ (show actual)

data Exp = One Exp | Two Exp Exp | Nil deriving (Eq, Show)

instance NFData Exp where
  rnf Nil = ()
  rnf (One c) = rnf c `seq` ()
  rnf (Two l r) = rnf l `seq` rnf r `seq` ()

makeExp :: Int -> Exp
makeExp 0 = Nil
makeExp n | n `mod` 2 == 1 = One (makeExp (n-1))
          | otherwise = let a = makeExp (n-1)
                        in
                         Two a a

main = do
  -- the value is 17 Exp cells large, but if we don't
  -- preserve sharing it becomes 2**17, which won't fit
  -- in a compact of 4096 bytes
  let val = makeExp 16
  maybeStr <- compactNewNoShare 4096 val
  case maybeStr of
    Just str -> assertFail "compact created even though it should not fit"
    Nothing -> do
      maybeStr2 <- compactNew 4096 val
      case maybeStr2 of
        Nothing -> assertFail "failed to create compact"
        Just str2 -> return ()

