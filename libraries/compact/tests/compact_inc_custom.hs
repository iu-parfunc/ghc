{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}

module Main where

import Control.Exception
import System.Mem

import Data.Compact.Incremental

import Control.DeepSeq
import GHC.Generics

assertFail :: String -> IO ()
assertFail msg = throwIO $ AssertionFailed msg

assertEquals :: (Eq a, Show a) => a -> a -> IO ()
assertEquals expected actual =
  if expected == actual then return ()
  else assertFail $ "expected " ++ (show expected)
       ++ ", got " ++ (show actual)

appendRec :: Compactable a => Compact b -> a -> IO a
appendRec str val = do
  str' <- compactAppendRecursively str val
  return $ compactGetRoot str'

data Manual a = MSimple Int Int Int | MPoly a | MRec (Manual a) | MNullary deriving (Eq, Show)

instance Compactable a => Compactable (Manual a) where
  compact str MNullary = compactAppendOne str MNullary

  -- no need to go recursive, we can ask ghc to generate the
  -- the code that enters x, y and z and then copy the
  -- entire thing into the compact
  compact str val@(MSimple !x !y !z) = compactAppendEvaled str val

  compact str (MRec r) = do
    !r' <- appendRec str r
    compactAppendOne str (MRec r')

  compact str (MPoly a) = do
    !a' <- appendRec str a
    compactAppendOne str (MPoly a')

data Automatic a = ASimple Int Int Int | APoly a | ARec (Automatic a) | ANullary deriving (Eq, Show)

instance NFData a => NFData (Automatic a) where
  rnf ANullary = ()
  rnf (ASimple !x !y !z) = ()
  rnf (ARec r) = rnf r `seq` ()
  rnf (APoly a) = rnf a `seq` ()

instance NFData a => Compactable (Automatic a) where
  compact = defaultCompactNFData

data FullyAutomatic a = FASimple Int Int Int | FAPoly a | FARec (FullyAutomatic a) | FANullary deriving (Eq, Show, Generic)

instance NFData a => NFData (FullyAutomatic a)

instance NFData a => Compactable (FullyAutomatic a) where
  compact = defaultCompactNFData

createRef :: (Int -> Int -> Int -> m c) -> (a -> m a) ->
             (m a -> m a) -> (m b) -> a -> (m c, m a, m a, m b)
createRef simple poly rec null val =
  (simple 7 42 (-128), poly val, rec (poly val), null)

appendMultiple :: (Compactable (m a), Compactable (m b), Compactable (m c)) =>
                  Compact () -> (Int -> Int -> Int -> m c) -> (a -> m a) ->
                  (m a -> m a) -> (m b) -> a ->
                  IO (Compact (m c, m a, m a, m b))
appendMultiple str simple poly rec null val = do
  !v1 <- appendRec str (simple 7 42 (-128))
  !v2 <- appendRec str (poly val)
  !v3 <- appendRec str (rec (poly val))
  !v4 <- appendRec str null
  compactAppendOne str (v1, v2, v3, v4)

testOne :: (Compactable (m a), Compactable (m Char), Compactable (m Int),
            Eq (m a), Eq (m Char), Eq (m Int), Show (m a), Show (m Char),
            Show (m Int)) =>
           Compact () -> (Int -> Int -> Int -> m Int) -> (a -> m a) ->
           (m a -> m a) -> (m Char) -> a -> IO ()
testOne str simple poly rec null val = do
  let ref = createRef simple poly rec null val
  str2 <- appendMultiple str simple poly rec null val
  assertEquals ref (compactGetRoot str2)
  performMajorGC
  assertEquals ref (compactGetRoot str2)

test :: Compact () -> IO ()
test str = do
  testOne str MSimple MPoly MRec MNullary "hello"
  testOne str ASimple APoly ARec ANullary "hello"
  testOne str FASimple FAPoly FARec FANullary "hello"

main = do
  str <- compactNew 8192 ()
  test str

