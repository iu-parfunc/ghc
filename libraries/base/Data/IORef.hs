{-# LANGUAGE Trustworthy #-} -- is it?
{-# LANGUAGE CPP, NoImplicitPrelude #-}

module Data.IORef
    (
        -- * IORefs
        IORef,                -- abstract, instance of: Eq, Typeable
        newIORef,
        readIORef,
        writeIORef,
        modifyIORef,
        modifyIORef',
        atomicModifyIORef,
        atomicModifyIORef',
        atomicWriteIORef,

#if !defined(__PARALLEL_HASKELL__)
        mkWeakIORef,
#endif
        ) where

import Data.IORef.IORef
