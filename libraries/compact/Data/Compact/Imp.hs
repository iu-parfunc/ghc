{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Data.Compact.Imp
-- Copyright   :  (c) The University of Glasgow 2001-2009
--                (c) Giovanni Campagna <gcampagn@cs.stanford.edu> 2015
-- License     :  BSD-style (see the file LICENSE)
--
-- Maintainer  :  libraries@haskell.org
-- Stability   :  unstable
-- Portability :  non-portable (GHC Extensions)
--
-- This module provides a data structure, called a Compact, for holding
-- a set of fully evaluated Haskell values in a consecutive block of
-- memory.
--
-- As the data fully evaluated and pure (thus immutable), it maintains
-- the invariant that no memory reference exist from objects inside the
-- Compact to objects outside, thus allowing very fast garbage collection
-- (at the expense of increased memory usage, because the entire set of
-- object is kept alive if any object is alive).
--
-- This is a private implementation detail of the package and should
-- not be imported directly
--
-- /Since: 1.0.0/

module Data.Compact.Imp(
  Compact(..),
  compactGetRoot,
  compactGetBuffer,
  compactResize,

  compactAppendEvaledInternal,

  SerializedCompact(..),
  withCompactPtrs,
  compactImport,
  compactImportByteStrings,
) where

-- Write down all GHC.Prim deps explicitly to keep them at minimum
import GHC.Prim (Compact#,
                 compactAppend#,
                 compactResize#,
                 compactGetFirstBlock#,
                 compactGetNextBlock#,
                 compactAllocateBlockAt#,
                 compactFixupPointers#,
                 touch#,
                 Addr#,
                 nullAddr#,
                 eqAddr#,
                 addrToAny#,
                 State#,
                 RealWorld,
                 Int#,
                 Word#,
                 )
-- We need to import Word from GHC.Types to see the representation
-- and to able to access the Word# to pass down the primops
import GHC.Types (IO(..), Word(..), isTrue#)
import GHC.Word (Word8)

import GHC.Ptr (Ptr(..), plusPtr)

import qualified Data.ByteString as ByteString
import Data.ByteString.Internal(toForeignPtr)
import Data.IORef(newIORef, readIORef, writeIORef)
import Foreign.ForeignPtr(withForeignPtr)
import Foreign.Marshal.Utils(copyBytes)

data Compact a = Compact Compact# Addr#

-- | 'compactGetRoot': retrieve the object that was stored in a Compact
compactGetRoot :: Compact a -> a
compactGetRoot (Compact _ obj) =
  case addrToAny# obj of
    (# a #) -> a

compactGetBuffer :: Compact a -> Compact#
compactGetBuffer (Compact buffer _) = buffer

addrIsNull :: Addr# -> Bool
addrIsNull addr = isTrue# (nullAddr# `eqAddr#` addr)

compactResize :: Compact a -> Word -> IO ()
compactResize (Compact oldBuffer _) (W# new_size) =
  IO (\s -> case compactResize# oldBuffer new_size s of
         (# s' #) -> (# s', () #) )

compactAppendEvaledInternal :: Compact# -> a -> Int# -> State# RealWorld ->
                        (# State# RealWorld, Compact a #)
compactAppendEvaledInternal buffer root share s =
  case compactAppend# buffer root share s of
    (# s', rootAddr #) -> (# s', Compact buffer rootAddr #)

data SerializedCompact a = SerializedCompact {
  serializedCompactGetBlockList :: [(Ptr a, Word)],
  serializedCompactGetRoot :: Ptr a
  }

mkBlockList :: Compact# -> [(Ptr a, Word)]
mkBlockList buffer = go (compactGetFirstBlock# buffer)
  where
    go :: (# Addr#, Word# #) -> [(Ptr a, Word)]
    go (# block, _ #) | addrIsNull block = []
    go (# block, size #) = let next = compactGetNextBlock# buffer block
               in
                mkBlock block size : go next

    mkBlock :: Addr# -> Word# -> (Ptr a, Word)
    mkBlock block size = (Ptr block, W# size)

withCompactPtrs :: Compact a -> (SerializedCompact a -> IO c) -> IO c
withCompactPtrs c@(Compact buffer rootAddr) func = do
  let serialized = SerializedCompact (mkBlockList buffer) (Ptr rootAddr)
  r <- func serialized
  IO (\s -> case touch# c s of
         s' -> (# s', r #) )

fixupPointers :: Addr# -> Addr# -> State# RealWorld -> (# State# RealWorld, Maybe (Compact a) #)
fixupPointers firstBlock rootAddr s =
  case compactFixupPointers# firstBlock rootAddr s of
    (# s', buffer, adjustedRoot #) ->
      if addrIsNull adjustedRoot then (# s', Nothing #)
      else (# s', Just $ Compact buffer adjustedRoot #)

compactImport :: SerializedCompact a -> (Ptr b -> Word -> IO ()) -> IO (Maybe (Compact a))

-- what we would like is
{-
 importCompactPtrs ((firstAddr, firstSize):rest) = do
   (firstBlock, compact) <- compactAllocateAt firstAddr firstSize 
 #nullAddr
   fillBlock firstBlock firstAddr firstSize
   let go prev [] = return ()
       go prev ((addr, size):rest) = do
         (block, _) <- compactAllocateAt addr size prev
         fillBlock block addr size
         go block rest
   go firstBlock rest
   if isTrue# (compactFixupPointers compact) then
     return $ Just compact
     else
     return Nothing

But we can't do that because IO Addr# is not valid (kind mismatch)
This check exists to prevent a polymorphic data constructor from using
an unlifted type (which would break GC) - it would not a problem for IO
because IO stores a function, not a value, but the kind check is there
anyway.
Note that by the reasoning, we cannot do IO (# Addr#, Word# #), nor
we can do IO (Addr#, Word#) (that would break the GC for real!)

And therefore we need to do everything with State# explicitly.
-}

-- just do shut up GHC
compactImport (SerializedCompact [] _) _ = return Nothing
compactImport (SerializedCompact ((Ptr firstAddr, W# firstSize):otherBlocks) (Ptr rootAddr)) filler =
  IO (\s0 -> case compactAllocateBlockAt# firstAddr firstSize nullAddr# s0 of
         (# s1, firstBlock #) ->
           case fillBlock firstBlock firstSize s1 of
             (# s2 #) ->
               case go firstBlock otherBlocks s2 of
                 (# s3 #) -> fixupPointers firstBlock rootAddr s3 )
  where
    -- the additional level of tupling in the result is to make sure that
    -- the cases above are strict, which ensures operations are done in the
    -- order they are specified
    -- (they are unboxed tuples, which represent nothing at the Cmm level
    -- so it is not a problem)
    fillBlock :: Addr# -> Word# -> State# RealWorld -> (# State# RealWorld #)
    fillBlock addr size s = case filler (Ptr addr) (W# size) of
      IO action -> case action s of
        (# s', _ #) -> (# s' #)

    go :: Addr# -> [(Ptr a, Word)] -> State# RealWorld -> (# State# RealWorld #)
    go _ [] s = (# s #)
    go previous ((Ptr addr, W# size):rest) s =
      case compactAllocateBlockAt# addr size previous s of
        (# s', block #) -> go block rest s'

sanityCheckByteStrings :: SerializedCompact a -> [ByteString.ByteString] -> Bool
sanityCheckByteStrings (SerializedCompact scl _) bsl = go scl bsl
  where
    go [] [] = True
    go (_:_) [] = False
    go [] (_:_) = False
    go ((_, size):scs) (bs:bss) =
      fromIntegral size == ByteString.length bs && go scs bss

compactImportByteStrings :: SerializedCompact a -> [ByteString.ByteString] ->
                          IO (Maybe (Compact a))
compactImportByteStrings serialized stringList =
  -- sanity check stringList first - if we throw an exception later we leak
  -- memory!
  if not (sanityCheckByteStrings serialized stringList) then
    return Nothing
  else do
    state <- newIORef stringList
    let filler :: Ptr Word8 -> Word -> IO ()
        filler to size = do
          -- this pattern match will never fail
          (next:rest) <- readIORef state
          let (fp, off, _) = toForeignPtr next
          withForeignPtr fp $ \from -> do
            copyBytes to (from `plusPtr` off) (fromIntegral size)
          writeIORef state rest
    compactImport serialized filler
