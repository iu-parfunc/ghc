/* -----------------------------------------------------------------------------
 *
 * (c) The GHC Team 1998-2014
 *
 * GC support for immutable non-GCed structures
 *
 * Documentation on the architecture of the Garbage Collector can be
 * found in the online commentary:
 *
 *   http://ghc.haskell.org/trac/ghc/wiki/Commentary/Rts/Storage/GC
 *
 * ---------------------------------------------------------------------------*/

#ifndef SM_CNF_H
#define SM_CNF_H

#include "BeginPrivate.h"

StgCompactNFData *compactNew   (Capability      *cap,
                                StgWord          size);
StgPtr            compactAppend(Capability       *cap,
                                StgCompactNFData *str,
                                StgClosure       *what,
                                StgWord           share);
StgPtr            compactAppendOne(Capability       *cap,
                                   StgCompactNFData *str,
                                   StgClosure       *what);
void              compactResize(Capability       *cap,
                                StgCompactNFData *str,
                                StgWord           new_size);
void              compactFree  (StgCompactNFData *str);
void              compactMarkKnown(StgCompactNFData *str);
StgWord           compactContains(StgCompactNFData *str,
                                  StgPtr            what);

StgCompactNFDataBlock *compactAllocateBlockAt(Capability            *cap,
                                              StgPtr                 addr,
                                              StgWord                size,
                                              StgCompactNFDataBlock *previous);
StgPtr                 compactFixupPointers  (StgCompactNFData      *str,
                                              StgClosure            *root);

INLINE_HEADER StgCompactNFDataBlock *objectGetCompactBlock (StgClosure *closure);
INLINE_HEADER StgCompactNFDataBlock *objectGetCompactBlock (StgClosure *closure)
{
    bdescr *object_block, *head_block;

    object_block = Bdescr((StgPtr)closure);

    ASSERT ((object_block->flags & BF_COMPACT) != 0);

    if (object_block->free == 0)
        head_block = object_block->link;
    else
        head_block = object_block;

    ASSERT ((head_block->flags & BF_COMPACT) != 0);

    return (StgCompactNFDataBlock*)(head_block->start);
}

INLINE_HEADER StgCompactNFData *objectGetCompact (StgClosure *closure);
INLINE_HEADER StgCompactNFData *objectGetCompact (StgClosure *closure)
{
    StgCompactNFDataBlock *block = objectGetCompactBlock (closure);
    return block->owner;
}

#include "EndPrivate.h"

#endif // SM_COMPACT_H