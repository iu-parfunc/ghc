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
#include "HeapAlloc.h"
#include "BuildId.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

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
#ifdef USE_STRIPED_ALLOCATOR
    block = allocGroupInChunk(RtsFlags.GcFlags.sharedChunk, n_blocks);
#else
    block = allocGroup(n_blocks);
#endif
    if (linkGeneration)
        dbl_link_onto(block, &g0->compact_objects);
    g0->n_compact_blocks += block->blocks;
    RELEASE_SM_LOCK;

    cap->total_allocated += aligned_size / sizeof(StgWord);

    self = (StgCompactNFDataBlock*) block->start;
    self->self = self;
    self->next = NULL;

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
    const StgWord8 *build_id;
    bdescr *bd;

    aligned_size = BLOCK_ROUND_UP(size + sizeof(StgCompactNFDataBlock)
                                  + sizeof(StgCompactNFDataBlock));
    block = compactAllocateBlock(cap, aligned_size, rtsTrue);

    self = (StgCompactNFData*)((W_)block + sizeof(StgCompactNFDataBlock));
    SET_INFO((StgClosure*)self, &stg_COMPACT_NFDATA_info);
    self->totalW = aligned_size / sizeof(StgWord);
    self->autoBlockW = aligned_size / sizeof(StgWord);
    self->nursery = block;
    self->last = block;

    build_id = getBinaryBuildId();
    memcpy(self->build_id, build_id, BUILD_ID_SIZE);

    block->owner = self;

    bd = Bdescr((P_)block);
    bd->free = (StgPtr)((W_)self + sizeof(StgCompactNFData));
    ASSERT (bd->free == (StgPtr)self + sizeofW(StgCompactNFData));

    return self;
}

static void
compactAppendBlock (Capability *cap, StgCompactNFData *str, StgWord aligned_size)
{
    StgCompactNFDataBlock *block;
    bdescr *bd;

    block = compactAllocateBlock(cap, aligned_size, rtsFalse);
    block->owner = str;

    str->last->next = block;
    str->last = block;
    if (str->nursery == NULL)
        str->nursery = block;

    bd = Bdescr((P_)block);
    bd->free = (StgPtr)((W_)block + sizeof(StgCompactNFDataBlock));
    ASSERT (bd->free == (StgPtr)block + sizeofW(StgCompactNFDataBlock));
}

void
compactResize (Capability *cap, StgCompactNFData *str, StgWord new_size)
{
    StgWord aligned_size;

    aligned_size = BLOCK_ROUND_UP(new_size + sizeof(StgCompactNFDataBlock));

    str->totalW += aligned_size / sizeof(StgWord);
    str->autoBlockW = aligned_size / sizeof(StgWord);

    compactAppendBlock(cap, str, aligned_size);
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
allocate_in_compact (StgCompactNFDataBlock *block, StgWord sizeW, StgPtr *at)
{
    bdescr *bd;
    StgPtr top;
    StgPtr free;

    bd = Bdescr((StgPtr)block);
    top = bd->start + BLOCK_SIZE_W * bd->blocks;
    if (bd->free + sizeW > top)
        return rtsFalse;

    free = bd->free;
    bd->free += sizeW;
    *at = free;

    return rtsTrue;
}

static rtsBool
block_is_full (StgCompactNFDataBlock *block)
{
    bdescr *bd;
    StgPtr top;
    StgWord sizeW;

    bd = Bdescr((StgPtr)block);
    top = bd->start + BLOCK_SIZE_W * bd->blocks;

    // We consider a block full if we could not fit
    // an entire closure with 1 payload item
    sizeW = sizeofW(StgHeader) + 1;
    return (bd->free + sizeW > top);
}

static void
allocate_loop (Capability *cap, StgCompactNFData *str, StgWord sizeW, StgPtr *at)
{
    rtsBool ok;

    while (str->nursery != NULL) {
        if (allocate_in_compact(str->nursery, sizeW, at))
            return;

        if (block_is_full (str->nursery))
            str->nursery = str->nursery->next;
    }

    compactAppendBlock(cap, str, str->autoBlockW);
    ASSERT (str->nursery != NULL);
    ok = allocate_in_compact(str->nursery, sizeW, at);
#if DEBUG
    ASSERT (ok);
#else
    // tell gcc that ok will be true at this point - this allows GCC
    // to infer that *at was properly initialized
    if (!ok)
        __builtin_unreachable();
#endif
}

static void
copy_tag (Capability *cap, StgCompactNFData *str, HashTable *hash, StgClosure **p, StgClosure *from, StgWord tag)
{
    StgPtr to;
    StgWord sizeW;

    sizeW = closure_sizeW(from);

    allocate_loop(cap, str, sizeW, &to);

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
}

STATIC_INLINE rtsBool
object_in_compact (StgCompactNFData *str, StgClosure *p)
{
    bdescr *bd;

    if (!HEAP_ALLOCED(p))
        return rtsFalse;

    bd = Bdescr((P_)p);
    return (bd->flags & BF_COMPACT) != 0 &&
        objectGetCompact(p) == str;
}

static void
simple_evacuate (Capability *cap, StgCompactNFData *str, HashTable *hash, StgClosure **p)
{
    StgWord tag;
    StgClosure *from;
    void *already;

    from = *p;
    tag = GET_CLOSURE_TAG(from);
    from = UNTAG_CLOSURE(from);

    // If the object referenced is already in this compact
    // (for example by reappending an object that was obtained
    // by compactGetRoot) then do nothing
    if (object_in_compact(str, from))
        return;

    // This object was evacuated already, return the existing
    // pointer
    if (hash != NULL &&
        (already = lookupHashTable (hash, (StgWord)from))) {
        *p = TAG_CLOSURE(tag, (StgClosure*)already);
        return;
    }

    switch (get_itbl(from)->type) {
    case BLACKHOLE:
        // If tag == 0, the indirectee is the TSO that claimed the tag
        //
        // Not useful and not NFData
        from = ((StgInd*)from)->indirectee;
        if (GET_CLOSURE_TAG(from) == 0) {
            debugBelch("Claimed but not updated BLACKHOLE in Compact, not normal form");
            return;
        }

        *p = from;
        return simple_evacuate(cap, str, hash, p);

    case IND:
    case IND_STATIC:
        // follow chains of indirections, don't evacuate them
        from = ((StgInd*)from)->indirectee;
        *p = from;
        // Evac.c uses a goto, but let's rely on a smart compiler
        // and get readable code instead
        return simple_evacuate(cap, str, hash, p);

    default:
        copy_tag(cap, str, hash, p, from, tag);
    }
}

static void
simple_scavenge_block (Capability *cap, StgCompactNFData *str, StgCompactNFDataBlock *block, HashTable *hash, StgPtr p)
{
    StgInfoTable *info;
    bdescr *bd = Bdescr((P_)block);

    while (p < bd->free) {
        ASSERT (LOOKS_LIKE_CLOSURE_PTR(p));
        info = get_itbl((StgClosure*)p);

        switch (info->type) {
        case CONSTR_1_0:
            simple_evacuate(cap, str, hash, &((StgClosure*)p)->payload[0]);
        case CONSTR_0_1:
            p += sizeofW(StgClosure) + 1;
            break;

        case CONSTR_2_0:
            simple_evacuate(cap, str, hash, &((StgClosure*)p)->payload[1]);
        case CONSTR_1_1:
            simple_evacuate(cap, str, hash, &((StgClosure*)p)->payload[0]);
        case CONSTR_0_2:
            p += sizeofW(StgClosure) + 2;
            break;

        case CONSTR:
        case PRIM:
        case CONSTR_NOCAF_STATIC:
        case CONSTR_STATIC:
        {
            StgPtr end;

            end = (P_)((StgClosure *)p)->payload + info->layout.payload.ptrs;
            for (p = (P_)((StgClosure *)p)->payload; p < end; p++) {
                simple_evacuate(cap, str, hash, (StgClosure **)p);
            }
            p += info->layout.payload.nptrs;
            break;
        }

        case IND:
        case BLACKHOLE:
        case IND_STATIC:
            // They get shortcircuited by simple_evaluate()
            barf("IND/BLACKHOLE in Compact");
            break;

        default:
            debugBelch("Invalid non-NFData closure in Compact\n");
        }
    }
}

static void
scavenge_loop (Capability *cap, StgCompactNFData *str, StgCompactNFDataBlock *first_block, HashTable *hash, StgPtr p)
{
    // Scavenge the first block
    simple_scavenge_block(cap, str, first_block, hash, p);

    // Now, if the nursery pointer did not change, we're done,
    // otherwise we need to scavenge the next block in the chain
    // we know that the next block was empty at the beginning,
    // so we need to scavenge it entirely
    // we also know that it does not contain a StgCompactNFData
    // because that is only in the absolute first block in the chain
    while (first_block != str->nursery) {
        first_block = first_block->next;
        simple_scavenge_block(cap, str, first_block, hash,
                              (P_)first_block + sizeof(StgCompactNFDataBlock));
    }
}

#ifdef DEBUG
static rtsBool
objectIsWHNFData (StgClosure *what)
{
    switch (get_itbl(what)->type) {
    case CONSTR:
    case CONSTR_1_0:
    case CONSTR_0_1:
    case CONSTR_2_0:
    case CONSTR_1_1:
    case CONSTR_0_2:
    case CONSTR_STATIC:
    case CONSTR_NOCAF_STATIC:
        return rtsTrue;

    case IND:
    case BLACKHOLE:
        return objectIsWHNFData(UNTAG_CLOSURE(((StgInd*)what)->indirectee));

    default:
        return rtsFalse;
    }
}
#endif

StgPtr
compactAppend (Capability *cap, StgCompactNFData *str, StgClosure *what, StgWord share)
{
    StgClosure *root;
    StgClosure *tagged_root;
    HashTable *hash;
    StgCompactNFDataBlock *evaced_block;

    ASSERT(objectIsWHNFData(UNTAG_CLOSURE(what)));

    tagged_root = what;
    simple_evacuate(cap, str, NULL, &tagged_root);

    root = UNTAG_CLOSURE(tagged_root);
    evaced_block = objectGetCompactBlock(root);

    if (share) {
        hash = allocHashTable ();
        insertHashTable(hash, (StgWord)UNTAG_CLOSURE(what), root);
    } else
        hash = NULL;

    scavenge_loop(cap, str, evaced_block, hash, (P_)root);

    if (share)
        freeHashTable(hash, NULL);

    return (StgPtr)tagged_root;
}

STATIC_INLINE void
maybe_adjust_one_indirection(StgCompactNFData *str, StgClosure **p)
{
    StgClosure *q;

    q = *p;
    q = UNTAG_CLOSURE(q);

    switch (get_itbl(q)->type) {
    case BLACKHOLE:
        // If tag == 0, the indirectee is the TSO that claimed the tag
        //
        // Not useful and not NFData
        q = ((StgInd*)q)->indirectee;
        ASSERT (GET_CLOSURE_TAG(q) != 0);
        *p = q;
        maybe_adjust_one_indirection(str, p);
        break;

    case IND:
    case IND_STATIC:
        q = ((StgInd*)q)->indirectee;
        *p = q;
        maybe_adjust_one_indirection(str, p);
        break;

    default:
        ASSERT (object_in_compact(str, q));
        break;
    }
}

static void
adjust_indirections(StgCompactNFData *str, StgClosure *what)
{
    StgInfoTable *info;

    info = get_itbl(what);
    switch (info->type) {
    case CONSTR_0_1:
    case CONSTR_0_2:
        break;

    case CONSTR_1_1:
    case CONSTR_1_0:
        maybe_adjust_one_indirection(str, &what->payload[0]);
        break;

    case CONSTR_2_0:
        maybe_adjust_one_indirection(str, &what->payload[0]);
        maybe_adjust_one_indirection(str, &what->payload[1]);
        break;

    case CONSTR:
    case PRIM:
    case CONSTR_STATIC:
    case CONSTR_NOCAF_STATIC:
    {
        nat i;

        for (i = 0; i < info->layout.payload.ptrs; i++)
            maybe_adjust_one_indirection(str, &what->payload[i]);

        break;
    }

    default:
        break;
    }
}

StgPtr
compactAppendOne (Capability *cap, StgCompactNFData *str, StgClosure *what)
{
    StgClosure *tagged_root;

    ASSERT(objectIsWHNFData(UNTAG_CLOSURE(what)));

    tagged_root = what;
    simple_evacuate(cap, str, NULL, &tagged_root);

    adjust_indirections(str, UNTAG_CLOSURE(tagged_root));

    return (StgPtr)tagged_root;
}

StgWord
compactContains (StgCompactNFData *str, StgPtr what)
{
    bdescr *bd;

    // This check is the reason why this needs to be
    // implemented in C instead of (possibly faster) Cmm
    //
    // (If the large address space patch goes in, OTOH,
    // we can write a Cmm version of HEAP_ALLOCED() and move
    // all this code to PrimOps.cmm - for x86_64 only)
    if (!HEAP_ALLOCED (what))
        return 0;

    // Note that we don't care about tags, they are eaten
    // away by the Bdescr operation anyway
    bd = Bdescr((P_)what);
    return (bd->flags & BF_COMPACT) != 0 &&
        (str == NULL || objectGetCompact((StgClosure*)what) == str);
}

#ifdef USE_STRIPED_ALLOCATOR
static StgCompactNFDataBlock *
compactDoAllocateBlockAt(Capability *cap, void *addr, StgWord aligned_size)
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
    block = allocGroupAt(addr, n_blocks);
    if (block == NULL) {
        // argh, we could not allocate where we wanted
        // allocate somewhere in the chunk where this compact comes from
        // and we'll do a pointer adjustment pass later
        block = allocGroupInChunk(mblock_address_get_chunk(addr), n_blocks);
    }
    // We do not link the new object into the generation ever
    // - we cannot let the GC know about this object until we're done
    // importing it and we have fixed up all info tables and stuff
    //
    // but we do update n_compact_blocks, otherwise memInventory()
    // in Sanity will think we have a memory leak, because it compares
    // the blocks he knows about with the blocks obtained by the
    // block allocator
    // (if by chance a memory leak does happen due to a bug somewhere
    // else, memInventory will also report that all compact blocks
    // associated with this compact are leaked - but they are not really,
    // we have a pointer to them and we're not losing track of it, it's
    // just we can't use the GC until we're done with the import)
    //
    // (That btw means that the high level import code must be careful
    // not to lose the pointer, so don't use the primops directly
    // unless you know what you're doing!)
    g0->n_compact_blocks += block->blocks;
    RELEASE_SM_LOCK;

    cap->total_allocated += aligned_size / sizeof(StgWord);

    // Unlike compactAllocate(), we do not initialize self here
    // because the caller is about to overwrite everything anyway
    self = (StgCompactNFDataBlock*) block->start;
    self->self = self;
    self->next = NULL;

    // Init bdescr unconditionally, it doesn't harm to do so for
    // later blocks anyway
    initBdescr(block, g0, g0);
    for ( ; n_blocks > 0; block++, n_blocks--)
        block->flags = BF_COMPACT;

    return self;
}

STATIC_INLINE rtsBool
can_alloc_group_at (void    *addr,
                    StgWord  size)
{
    nat chunk;

    if (BLOCK_ROUND_DOWN (addr) != addr)
        return rtsFalse;

    if (size >= BLOCKS_PER_MBLOCK * BLOCK_SIZE) {
        if (FIRST_BLOCK(MBLOCK_ROUND_DOWN(addr)) != (void*)addr)
            return rtsFalse;
    } else {
        if (MBLOCK_ROUND_DOWN(addr) != MBLOCK_ROUND_DOWN((W_)addr + size))
            return rtsFalse;
    }

    chunk = mblock_address_get_chunk(addr);
    if (chunk == 0 || chunk > MBLOCK_NUM_CHUNKS)
        return rtsFalse;

    return rtsTrue;
}
#endif // USE_STRIPED_ALLOCATOR

StgCompactNFDataBlock *
compactAllocateBlockAt(Capability            *cap,
                       StgPtr                 addr,
                       StgWord                size,
                       StgCompactNFDataBlock *previous)
{
    StgWord aligned_size;
    StgCompactNFDataBlock *block;
    bdescr *bd;

    aligned_size = BLOCK_ROUND_UP(size);

#ifdef USE_STRIPED_ALLOCATOR
    // Sanity check the address before making bogus calls to the
    // block allocator
    if (can_alloc_group_at(addr, aligned_size)) {
        block = compactDoAllocateBlockAt(cap, addr, aligned_size);
    } else {
        // See above for why this is rtsFalse and not "previous == NULL"
        block = compactAllocateBlock(cap, aligned_size, rtsFalse);
    }
#else
    block = compactAllocateBlock(cap, aligned_size, rtsFalse);
#endif

    if ((P_)block != addr && previous != NULL)
        previous->next = block;

    bd = Bdescr((P_)block);
    bd->free = (P_)((W_)bd->free + size);

    return block;
}

STATIC_INLINE rtsBool
any_needs_fixup(StgCompactNFDataBlock *block)
{
    // ->next pointers are always valid, even if some blocks were
    // not allocated where we want them, because compactAllocateAt()
    // will take care to adjust them

    do {
        if (block->self != block)
            return rtsTrue;
        block = block->next;
    } while (block);

    return rtsFalse;
}

STATIC_INLINE StgCompactNFDataBlock *
find_pointer(StgCompactNFData *str, StgClosure *q)
{
    StgCompactNFDataBlock *block;
    StgWord address = (W_)q;

    block = (StgCompactNFDataBlock*)((W_)str - sizeof(StgCompactNFDataBlock));
    do {
        bdescr *bd;
        StgWord size;

        bd = Bdescr((P_)block);
        size = (W_)bd->free - (W_)bd->start;
        if ((W_)block->self <= address &&
            address < (W_)block->self + size) {
            return block;
        }

        block = block->next;
    } while(block);

    // We should never get here
    return NULL;
}

static rtsBool
fixup_one_pointer(StgCompactNFData *str, StgClosure **p)
{
    StgWord tag;
    StgClosure *q;
    StgCompactNFDataBlock *block;

    q = *p;
    tag = GET_CLOSURE_TAG(q);
    q = UNTAG_CLOSURE(q);

    block = find_pointer(str, q);
    if (block == NULL)
        return rtsFalse;
    if (block == block->self)
        return rtsTrue;

    q = (StgClosure*)((W_)q - (W_)block->self + (W_)block);
    *p = TAG_CLOSURE(tag, q);

    return rtsTrue;
}

static rtsBool
fixup_block(StgCompactNFData *str, StgCompactNFDataBlock *block)
{
    StgInfoTable *info;
    bdescr *bd;
    StgPtr p;

    bd = Bdescr((P_)block);
    p = bd->start + sizeofW(StgCompactNFDataBlock);
    while (p < bd->free) {
        ASSERT (LOOKS_LIKE_CLOSURE_PTR(p));
        info = get_itbl((StgClosure*)p);

        switch (info->type) {
        case CONSTR_1_0:
            if (!fixup_one_pointer(str, &((StgClosure*)p)->payload[0]))
                return rtsFalse;
        case CONSTR_0_1:
            p += sizeofW(StgClosure) + 1;
            break;

        case CONSTR_2_0:
            if (!fixup_one_pointer(str, &((StgClosure*)p)->payload[1]))
                return rtsFalse;
        case CONSTR_1_1:
            if (!fixup_one_pointer(str, &((StgClosure*)p)->payload[0]))
                return rtsFalse;
        case CONSTR_0_2:
            p += sizeofW(StgClosure) + 2;
            break;

        case CONSTR:
        case PRIM:
        case CONSTR_STATIC:
        case CONSTR_NOCAF_STATIC:
        {
            StgPtr end;

            end = (P_)((StgClosure *)p)->payload + info->layout.payload.ptrs;
            for (p = (P_)((StgClosure *)p)->payload; p < end; p++) {
                if (!fixup_one_pointer(str, (StgClosure **)p))
                    return rtsFalse;
            }
            p += info->layout.payload.nptrs;
            break;
        }

        case COMPACT_NFDATA:
            if (p == (bd->start + sizeofW(StgCompactNFDataBlock))) {
                // Ignore the COMPACT_NFDATA header
                // (it will be fixed up later)
                p += sizeofW(StgCompactNFData);
                break;
            }

            // fall through

        default:
            debugBelch("Invalid non-NFData closure in Compact\n");
            return rtsFalse;
        }
    }

    return rtsTrue;
}

static rtsBool
fixup_loop(StgCompactNFData *str, StgCompactNFDataBlock *block)
{
    do {
        if (!fixup_block(str, block))
            return rtsFalse;

        block = block->next;
    } while(block);

    return rtsTrue;
}

static void
fixup_late(StgCompactNFData *str, StgCompactNFDataBlock *block)
{
    bdescr *bd;
    StgCompactNFDataBlock *nursery;
    StgCompactNFDataBlock *last;

    nursery = block;
    do {
        bd = Bdescr((P_)block);
        if (bd->free != bd->start)
            nursery = block;
        last = block;

        block->self = block;
        block->owner = str;
        block = block->next;
    } while(block);

    str->nursery = nursery;
    str->last = last;
}

static StgClosure *
maybe_fixup_internal_pointers (StgCompactNFDataBlock *block,
                               StgCompactNFData      *str,
                               StgClosure            *root)
{
    rtsBool ok;
    StgClosure **proot;

    // Check for fast path
    if (!any_needs_fixup(block))
        return root;

    debugBelch("Compact imported at the wrong address, will fix up"
               " internal pointers\n");

    // I am PROOT!
    proot = &root;

    ok = fixup_loop(str, block);
    if (ok)
        ok = fixup_one_pointer(str, proot);
    if (!ok)
        *proot = NULL;

    return *proot;
}

static rtsBool
maybe_fixup_info_tables (StgCompactNFDataBlock *block,
                         StgCompactNFData      *str)
{
    const StgWord8 *build_id;

    build_id = getBinaryBuildId();
    if (memcmp(str->build_id, build_id, BUILD_ID_SIZE) == 0)
        return rtsTrue;

    // Slow path: reconstruct info tables
    debugBelch("Binary versions do not match, slow path to adjust"
               " info tables\n");

    // FIXME
    (void)block;

    return rtsFalse;
}

StgPtr
compactFixupPointers(StgCompactNFData *str,
                     StgClosure       *root)
{
    StgCompactNFDataBlock *block;
    bdescr *bd;

    block = (StgCompactNFDataBlock*)((W_)str - sizeof(StgCompactNFDataBlock));

    if (maybe_fixup_info_tables(block, str))
        root = maybe_fixup_internal_pointers(block, str, root);
    else
        root = NULL;

    // Do the late fixup even if we did not fixup all
    // internal pointers or info tables, we need that for GC and Sanity
    fixup_late(str, block);

    // Now we're ready to let the GC, Sanity, the profiler
    // etc. know about this object
    bd = Bdescr((P_)block);

    ACQUIRE_SM_LOCK;
    dbl_link_onto(bd, &g0->compact_objects);
    RELEASE_SM_LOCK;

    return (StgPtr)root;
}
