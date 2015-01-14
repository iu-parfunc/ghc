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

#include "PosixSource.h"
#include <string.h>
#include "Rts.h"
#include "RtsUtils.h"

#include "Capability.h"
#include "GC.h"
#include "Storage.h"
#include "CNF.h"
#include "Hash.h"

static StgCompactNFDataBlock *
compactAllocateBlock(Capability *cap, StgWord aligned_size, rtsBool linkGeneration)
{
    StgCompactNFDataBlock *self;
    bdescr *block;
    nat n_blocks;

    n_blocks = aligned_size / BLOCK_SIZE;

    // Attempting to allocate an object larger than maxHeapSize
    // should definitely be disallowed.  (bug #1791)
    if ((RtsFlags.GcFlags.maxHeapSize > 0 &&
         n_blocks >= RtsFlags.GcFlags.maxHeapSize) ||
        n_blocks >= HS_INT32_MAX)   // avoid overflow when
                                    // calling allocGroup() below
    {
        heapOverflow();
        // heapOverflow() doesn't exit (see #2592), but we aren't
        // in a position to do a clean shutdown here: we
        // either have to allocate the memory or exit now.
        // Allocating the memory would be bad, because the user
        // has requested that we not exceed maxHeapSize, so we
        // just exit.
        stg_exit(EXIT_HEAPOVERFLOW);
    }

    ACQUIRE_SM_LOCK;
    block = allocGroup(n_blocks);
    if (linkGeneration)
        dbl_link_onto(block, &g0->compact_objects);
    g0->n_compact_blocks += block->blocks;
    RELEASE_SM_LOCK;

    cap->total_allocated += aligned_size / sizeof(StgWord);

    self = (StgCompactNFDataBlock*) block->start;
    self->next = NULL;

    if (linkGeneration)
        initBdescr(block, g0, g0);
    for ( ; n_blocks > 0; block++, n_blocks--)
        block->flags = BF_COMPACT;

    return self;
}

void
compactFree(StgCompactNFData *str)
{
    bdescr *bd;
    StgCompactNFDataBlock *block, *next;

    block = (StgCompactNFDataBlock*) ((W_)str - sizeof(StgCompactNFDataBlock));
    for ( ; block; block = next) {
        next = block->next;
        bd = Bdescr((StgPtr)block);
        ASSERT((bd->flags & BF_EVACUATED) == 0);
        freeGroup(bd);
    }
}

void
compactMarkKnown(StgCompactNFData *str)
{
    bdescr *bd;
    StgCompactNFDataBlock *block;

    block = (StgCompactNFDataBlock*) ((W_)str - sizeof(StgCompactNFDataBlock));
    for ( ; block; block = block->next) {
        bd = Bdescr((StgPtr)block);
        bd->flags |= BF_KNOWN;
    }
}

StgCompactNFData *
compactNew (Capability *cap, StgWord size)
{
    StgWord aligned_size;
    StgCompactNFDataBlock *block;
    StgCompactNFData *self;
    bdescr *bd;

    aligned_size = BLOCK_ROUND_UP(size + sizeof(StgCompactNFDataBlock)
                                  + sizeof(StgCompactNFDataBlock));
    block = compactAllocateBlock(cap, aligned_size, rtsTrue);

    self = (StgCompactNFData*)((W_)block + sizeof(StgCompactNFDataBlock));
    SET_INFO((StgClosure*)self, &stg_COMPACT_NFDATA_info);
    self->totalW = aligned_size / sizeof(StgWord);
    self->nursery = block;
    self->last = block;
    block->owner = self;

    bd = Bdescr((P_)block);
    bd->free = (StgPtr)((W_)self + sizeof(StgCompactNFData));
    ASSERT (bd->free == (StgPtr)self + sizeofW(StgCompactNFData));

    return self;
}

void
compactResize (Capability *cap, StgCompactNFData *str, StgWord new_size)
{
    StgWord aligned_size;
    StgCompactNFDataBlock *block;
    bdescr *bd;

    aligned_size = BLOCK_ROUND_UP(new_size + sizeof(StgCompactNFDataBlock));
    block = compactAllocateBlock(cap, aligned_size, rtsFalse);

    str->totalW += aligned_size / sizeof(StgWord);
    block->owner = str;

    str->last->next = block;
    str->last = block;

    bd = Bdescr((P_)block);
    bd->free = (StgPtr)((W_)block + sizeof(StgCompactNFDataBlock));
    ASSERT (bd->free == (StgPtr)block + sizeofW(StgCompactNFDataBlock));
}

/* This is a simple reimplementation of the copying GC.
   One could be tempted to reuse the actual GC code here, but he
   would quickly find out that it would bring all the generational
   GC complexity for no need at all.

   Plus, we don't need to scavenge/evacuate all kinds of weird
   objects here, just constructors and primitives. Thunks are
   expected to be evaluated before appending by the API layer
   (in Haskell, above the primop which is implemented here).
   Also, we have a different policy for large objects: instead
   of relinking to the new large object list, we fully copy
   them inside the compact and scavenge them normally.

   Note that if we allowed thunks and lazy evaluation the compact
   would be a mutable object, which would create all sorts of
   GC problems (besides, evaluating a thunk could exaust the
   compact space or yield an invalid object, and we would have
   no way to signal that to the user)

   Just like the real evacuate/scavenge pairs, we need to handle
   object loops. We would want to use the same strategy of rewriting objects
   with forwarding pointer, but in a real GC, at the end the
   blocks from the old space are dropped (dropping all forwarding
   pointers at the same time), which we can't do here as we don't
   know all pointers to the objects being evacuated. Also, in parallel
   we don't know which other threads are evaluating the thunks
   that we just corrupted at the same time.

   So instead we use a hash table of "visited" objects, and add
   the pointer as we copy it. To reduce the overhead, we also offer
   a version of the API that does not preserve sharing (TODO).

   You might be tempted to replace the objects with StdInd to
   the object in the compact, but you would be wrong: the haskell
   code assumes that objects in the heap only become more evaluated
   (thunks to blackholes to inds to actual objects), and in
   particular it assumes that if a pointer is tagged the object
   is directly referenced and the values can be read directly,
   without entering the closure.

   FIXME: any better idea than the hash table?
*/

static void
unroll_memcpy(StgPtr to, StgPtr from, StgWord size)
{
    for (; size > 0; size--)
        *(to++) = *(from++);
}

static rtsBool
atomic_allocate (StgCompactNFDataBlock *block, StgWord sizeW, StgPtr *at)
{
    bdescr *bd;
    StgPtr top;
    StgPtr expected_free, free;

    // There might be a better way to do this

    bd = Bdescr((StgPtr)block);
    top = bd->start + BLOCK_SIZE_W * bd->blocks;
 retry:
    if (bd->free + sizeW > top)
        return rtsFalse;

    expected_free = bd->free;
    free = (StgPtr)cas((StgVolatilePtr)&bd->free, (StgWord)expected_free,
                       (StgWord)(expected_free + sizeW));

    if (free != expected_free)
        goto retry;

    *at = free;

    return rtsTrue;
}

static rtsBool
allocate_loop (StgCompactNFData *str, StgWord sizeW, StgPtr *at)
{
    while (str->nursery != NULL) {
        if (atomic_allocate(str->nursery, sizeW, at))
            return rtsTrue;

        // There is no space in this block, treat it as fully occupied
        // and move to the next in the chain
        // This is slightly incorrect in that we could fit a smaller
        // closure in the free space still in the nursery, but to be
        // able to revert failed appends we maintain the invariant that
        // only the nursery is partially occupied, everything before
        // it is full and everything after is empty (so the revert code
        // restores the nursery to the values it had at the beginning
        // and empties all blocks following it)

        // this is not threadsafe, because a thread failing
        // to append can revert the concurrent append of a second thread
        // (which would lead to corruption because the second thread
        // thinks it has a good pointer)
        // the only correct way to fix this (without dealing with holes
        // in the blocks) is to lock the entire structure around appends,
        // which is undesirable
        str->nursery = str->nursery->next;
    }

    return rtsFalse;
}
static rtsBool
copy_tag (StgCompactNFData *str, HashTable *hash, StgClosure **p, StgClosure *from, StgWord tag)
{
    StgPtr to;
    StgWord sizeW;

    sizeW = closure_sizeW(from);

    if (!allocate_loop(str, sizeW, &to))
        return rtsFalse;

    // unroll memcpy for small sizes because we can
    // benefit of known alignment
    // (32 extracted from my magic hat)
    if (sizeW < 32)
        unroll_memcpy(to, (StgPtr)from, sizeW);
    else
        memcpy(to, from, sizeW * sizeof(StgWord));

    if (hash != NULL)
        insertHashTable(hash, (StgWord)from, to);
    *p = TAG_CLOSURE(tag, (StgClosure*)to);

    return rtsTrue;
}

STATIC_INLINE rtsBool
object_in_compact (StgCompactNFData *str, StgClosure *p)
{
    bdescr *bd = Bdescr((P_)p);

    return (bd->flags & BF_COMPACT) != 0 &&
        objectGetCompact(p) == str;
}

static rtsBool
simple_evacuate (StgCompactNFData *str, HashTable *hash, StgClosure **p)
{
    StgWord tag;
    StgClosure *from;
    void *already;

    from = *p;
    tag = GET_CLOSURE_TAG(from);
    from = UNTAG_CLOSURE(from);

    // Do nothing on static closures, just reference them.
    // They would not be GCed anyway. And in practice we should
    // not see them (because they are THUNKs or FUNs), but
    // we could see CHARLIKE and INTLIKE
    if (!HEAP_ALLOCED(from))
        return rtsTrue;

    // If the object referenced is already in this compact
    // (for example by reappending an object that was obtained
    // by compactGetRoot) then do nothing
    if (object_in_compact(str, from))
        return rtsTrue;

    // This object was evacuated already, return the existing
    // pointer
    if (hash != NULL &&
        (already = lookupHashTable (hash, (StgWord)from))) {
        *p = TAG_CLOSURE(tag, (StgClosure*)already);
        return rtsTrue;
    }

    switch (get_itbl(from)->type) {
    case BLACKHOLE:
        // If tag == 0, the indirectee is the TSO that claimed the tag
        //
        // Not useful and not NFData
        from = ((StgInd*)from)->indirectee;
        if (GET_CLOSURE_TAG(from) == 0)
            return rtsFalse;
        *p = from;
        return simple_evacuate(str, hash, p);

    case IND:
        // follow chains of indirections, don't evacuate them
        from = ((StgInd*)from)->indirectee;
        *p = from;
        // Evac.c uses a goto, but let's rely on a smart compiler
        // and get readable code instead
        return simple_evacuate(str, hash, p);

    default:
        return copy_tag(str, hash, p, from, tag);
    }
}

static rtsBool
simple_scavenge_block (StgCompactNFData *str, StgCompactNFDataBlock *block, HashTable *hash, StgPtr p)
{
    StgInfoTable *info;
    bdescr *bd = Bdescr((P_)block);

    while (p < bd->free) {
        ASSERT (LOOKS_LIKE_CLOSURE_PTR(p));
        info = get_itbl((StgClosure*)p);

        switch (info->type) {
        case CONSTR_1_0:
            if (!simple_evacuate(str, hash, &((StgClosure*)p)->payload[0]))
                return rtsFalse;
        case CONSTR_0_1:
            p += sizeofW(StgClosure) + 1;
            break;

        case CONSTR_2_0:
            if (!simple_evacuate(str, hash, &((StgClosure*)p)->payload[1]))
                return rtsFalse;
        case CONSTR_1_1:
            if (!simple_evacuate(str, hash, &((StgClosure*)p)->payload[0]))
                return rtsFalse;
        case CONSTR_0_2:
            p += sizeofW(StgClosure) + 2;
            break;

        case CONSTR:
        case PRIM:
        {
            StgPtr end;

            end = (P_)((StgClosure *)p)->payload + info->layout.payload.ptrs;
            for (p = (P_)((StgClosure *)p)->payload; p < end; p++) {
                if (!simple_evacuate(str, hash, (StgClosure **)p))
                    return rtsFalse;
            }
            p += info->layout.payload.nptrs;
            break;
        }

        case IND:
        case BLACKHOLE:
            if (!simple_evacuate(str, hash, &((StgInd*)p)->indirectee))
                return rtsFalse;
            p += sizeofW(StgInd);
            break;

        default:
            debugBelch("Invalid non-NFData closure in Compact\n");
            return rtsFalse;
        }
    }

    return rtsTrue;
}

static rtsBool
scavenge_loop (StgCompactNFData *str, StgCompactNFDataBlock *first_block, HashTable *hash, StgPtr p)
{
    // Scavenge the first block
    if (!simple_scavenge_block(str, first_block, hash, p))
        return rtsFalse;

    // Now, if the nursery pointer did not change, we're done,
    // otherwise we need to scavenge the next block in the chain
    // we know that the next block was empty at the beginning,
    // so we need to scavenge it entirely
    // we also know that it does not contain a StgCompactNFData
    // because that is only in the absolute first block in the chain
    while (first_block != str->nursery) {
        first_block = first_block->next;
        if (!simple_scavenge_block(str, first_block, hash,
                                   (P_)first_block + sizeof(StgCompactNFDataBlock)))
            return rtsFalse;
    }

    return rtsTrue;
}

StgPtr
compactAppend (StgCompactNFData *str, StgClosure *what, StgWord share)
{
    rtsBool ok;
    StgClosure *root;
    StgClosure *tagged_root;
    StgPtr start;
    HashTable *hash;
    StgCompactNFDataBlock *initial_nursery;
    bdescr *nursery_bd;
    bdescr *evaced_bd;
    StgCompactNFDataBlock *evaced_block;

    // If what is not heap alloced (it is a static CONSTR)
    // we will do nothing in simple_evacuate. But let's not
    // bother at all to set up the allocation system
    // (We ignore the tag because it doesn't make a difference as
    // far as heap_alloced is concerned)
    if (!HEAP_ALLOCED(what))
        return (StgPtr)what;

    initial_nursery = str->nursery;
    ASSERT (initial_nursery != NULL);
    nursery_bd = Bdescr((P_)initial_nursery);
    start = nursery_bd->free;
    tagged_root = what;
    if (!simple_evacuate(str, NULL, &tagged_root))
        return NULL;

    root = UNTAG_CLOSURE(tagged_root);
    // evaced_block can be different from initial_nursery
    // in case there wasn't enough space for the first
    // evacuate
    evaced_block = objectGetCompactBlock(root);

    if (share) {
        hash = allocHashTable ();
        insertHashTable(hash, (StgWord)UNTAG_CLOSURE(what), root);
    } else
        hash = NULL;

    ok = scavenge_loop(str, evaced_block, hash, (P_)root);

    if (share)
        freeHashTable(hash, NULL);

    // Return the tagged pointer for outside use, or NULL
    // if failed
    if (__builtin_expect(ok, rtsTrue)) {
        return (StgPtr)tagged_root;
    } else {
        // Undo any partial allocation
        nursery_bd->free = start;
        str->nursery = initial_nursery;
        for (initial_nursery = initial_nursery->next; initial_nursery;
             initial_nursery = initial_nursery->next) {
            nursery_bd = Bdescr((P_)initial_nursery);
            nursery_bd->free = (P_)nursery_bd->start + sizeofW(StgCompactNFDataBlock);
        }

        return NULL;
    }
}
