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

#ifndef SM_STRUCT_H
#define SM_STRUCT_H

#include "BeginPrivate.h"

StgNFDataStruct *structNew   (Capability      *cap,
                              StgWord          size);
StgPtr           structAppend(StgNFDataStruct *str,
                              StgClosure      *what);
StgNFDataStruct *structResize(Capability      *cap,
                              StgNFDataStruct *str,
                              StgWord          new_size);

INLINE_HEADER StgNFDataStruct *objectGetStruct (StgClosure *closure);
INLINE_HEADER StgNFDataStruct *objectGetStruct (StgClosure *closure)
{
    bdescr *object_block, *head_block;

    object_block = Bdescr((StgPtr)closure);

    ASSERT ((object_block->flags & BF_STRUCT) != 0);

    if (object_block->free == 0)
        head_block = object_block->link;
    else
        head_block = object_block;

    ASSERT ((head_block->flags & BF_STRUCT) != 0);

    return (StgNFDataStruct*)(head_block->start);
}

#endif // SM_STRUCT_H
