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

#define _GNU_SOURCE

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
#include "BlockAlloc.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif
#include <dlfcn.h>
#include <endian.h>

// FIXME thread safety?
// addr -> string
static HashTable *own_symbol_table;
// build id -> HashTable (addr -> addr)
static HashTable *foreign_conversion_table;

static int
compareBuildId(StgWord key1, StgWord key2)
{
    const StgWord8 *build_id1 = (StgWord8*)key1;
    const StgWord8 *build_id2 = (StgWord8*)key2;

    return memcmp(build_id1, build_id2, BUILD_ID_SIZE);
}

void
initCompact(void)
{
    own_symbol_table = allocHashTable();
    foreign_conversion_table = allocHashTable_((HashFunction *)hashBuildId,
                                               compareBuildId);
}

static void
build_id_table_free (void *data)
{
    HashTable *inner = data;
    freeHashTable(inner, NULL);
}

void
exitCompact(void)
{
    freeHashTable(own_symbol_table, stgFree);
    freeHashTable(foreign_conversion_table, build_id_table_free);
}

typedef enum {
    ALLOCATE_APPEND,
    ALLOCATE_NEW,
    ALLOCATE_IMPORT
} AllocateOp;

static StgCompactNFDataBlock *
compactAllocateBlock(Capability            *cap,
                     StgWord                aligned_size,
                     void                  *addr,
                     StgCompactNFDataBlock *first,
                     AllocateOp             operation)
{
    StgCompactNFDataBlock *self;
    bdescr *block;
    nat n_blocks;
    generation *g;

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

    // It is imperative that first is the first block in the compact
    // (or NULL if the compact does not exist yet)
    // because the evacuate code does not update the generation of
    // blocks other than the first (so we would get the statistics
    // wrong and crash in Sanity)
    if (first != NULL) {
        block = Bdescr((P_)first);
        g = block->gen;
    } else {
        g = g0;
    }

    ACQUIRE_SM_LOCK;
#ifdef USE_STRIPED_ALLOCATOR
    if (addr != NULL) {
        block = allocGroupAt(addr, n_blocks);
        if (block == NULL) {
            // argh, we could not allocate where we wanted
            // allocate somewhere in the chunk where this compact comes from
            // and we'll do a pointer adjustment pass later
            block = allocGroupInChunk(mblock_address_get_chunk(addr), n_blocks);
        }
    } else {
        block = allocGroupInChunk(RtsFlags.GcFlags.sharedChunk, n_blocks);
    }
#else
    ASSERT (addr == NULL);
    block = allocGroup(n_blocks);
#endif
    switch (operation) {
    case ALLOCATE_NEW:
        ASSERT (first == NULL);
        ASSERT (g == g0);
        dbl_link_onto(block, &g0->compact_objects);
        g->n_compact_blocks += block->blocks;
        break;

    case ALLOCATE_IMPORT:
        ASSERT (first == NULL);
        ASSERT (g == g0);
        g->n_compact_blocks_in_import += block->blocks;
        break;

    case ALLOCATE_APPEND:
        g->n_compact_blocks += block->blocks;
        break;

    default:
#ifdef DEBUG
        ASSERT(!"code should not be reached");
#else
        __builtin_unreachable();
#endif
    }
    RELEASE_SM_LOCK;

    cap->total_allocated += aligned_size / sizeof(StgWord);

    self = (StgCompactNFDataBlock*) block->start;
    self->self = self;
    self->next = NULL;

    initBdescr(block, g, g);
    for ( ; n_blocks > 0; block++, n_blocks--)
        block->flags = BF_COMPACT;

    return self;
}

static inline StgCompactNFDataBlock *
compactGetFirstBlock(StgCompactNFData *str)
{
    return (StgCompactNFDataBlock*) ((W_)str - sizeof(StgCompactNFDataBlock));
}

static inline StgCompactNFData *
firstBlockGetCompact(StgCompactNFDataBlock *block)
{
    return (StgCompactNFData*) ((W_)block + sizeof(StgCompactNFDataBlock));
}

static void
freeBlockChain(StgCompactNFDataBlock *block)
{
    StgCompactNFDataBlock *next;
    bdescr *bd;

    for ( ; block; block = next) {
        next = block->next;
        bd = Bdescr((StgPtr)block);
        ASSERT((bd->flags & BF_EVACUATED) == 0);
        freeGroup(bd);
    }
}

void
compactFree(StgCompactNFData *str)
{
    StgCompactNFDataBlock *block;

    if (str->symbols_hash)
        freeHashTable(str->symbols_hash, NULL);

    block = compactGetFirstBlock(str);
    freeBlockChain(block);
}

void
compactMarkKnown(StgCompactNFData *str)
{
    bdescr *bd;
    StgCompactNFDataBlock *block;

    block = compactGetFirstBlock(str);
    for ( ; block; block = block->next) {
        bd = Bdescr((StgPtr)block);
        bd->flags |= BF_KNOWN;
    }
}

StgWord
countCompactBlocks(bdescr *outer)
{
    StgCompactNFDataBlock *block;
    W_ count;

    count = 0;
    while (outer) {
        bdescr *inner;

        block = (StgCompactNFDataBlock*)(outer->start);
        do {
            inner = Bdescr((P_)block);
            ASSERT (inner->flags & BF_COMPACT);

            count += inner->blocks;
            block = block->next;
        } while(block);

        outer = outer->link;
    }

    return count;
}

#ifdef USE_STRIPED_ALLOCATOR
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

static inline StgCompactNFDataBlock *
compactAllocateBlockAtInternal (Capability *cap,
                                StgPtr      addr,
                                StgWord     aligned_size,
                                AllocateOp  operation)
{
    // Sanity check the address before making bogus calls to the
    // block allocator
    if (can_alloc_group_at(addr, aligned_size))
        return compactAllocateBlock(cap, aligned_size, addr, NULL, operation);
    else
        return compactAllocateBlock(cap, aligned_size, NULL, NULL, operation);
}

#else

static inline StgCompactNFDataBlock *
compactAllocateBlockAtInternal (Capability *cap,
                                StgPtr      addr,
                                StgWord     aligned_size,
                                AllocateOp  operation)
{
    return compactAllocateBlock(cap, aligned_size, NULL, NULL, operation);
}

#endif // USE_STRIPED_ALLOCATOR

StgCompactNFData *
compactNew (Capability *cap, StgWord size, StgPtr addr_hint)
{
    StgWord aligned_size;
    StgCompactNFDataBlock *block;
    StgCompactNFData *self;
    const StgWord8 *build_id;
    bdescr *bd;

    aligned_size = BLOCK_ROUND_UP(size + sizeof(StgCompactNFDataBlock)
                                  + sizeof(StgCompactNFDataBlock));
    if (aligned_size >= BLOCK_SIZE * BLOCKS_PER_MBLOCK)
        aligned_size = BLOCK_SIZE * BLOCKS_PER_MBLOCK;

    if (addr_hint != NULL)
        block = compactAllocateBlockAtInternal(cap, addr_hint, aligned_size,
                                               ALLOCATE_NEW);
    else
        block = compactAllocateBlock(cap, aligned_size, NULL, NULL,
                                     ALLOCATE_NEW);

    self = firstBlockGetCompact(block);
    SET_INFO((StgClosure*)self, &stg_COMPACT_NFDATA_info);
    self->totalDataW = aligned_size / sizeof(StgWord);
    self->autoBlockW = aligned_size / sizeof(StgWord);
    self->nursery = block;
    self->last = block;
    self->symbols = NULL;
    self->symbols_hash = NULL;
    self->symbols_serial = 0;

    build_id = getBinaryBuildId();
    memcpy(self->build_id, build_id, BUILD_ID_SIZE);

    block->owner = self;

    bd = Bdescr((P_)block);
    bd->free = (StgPtr)((W_)self + sizeof(StgCompactNFData));
    ASSERT (bd->free == (StgPtr)self + sizeofW(StgCompactNFData));

    self->totalW = bd->blocks * BLOCK_SIZE_W;

    return self;
}

static StgCompactNFDataBlock *
compactAppendBlock (Capability *cap, StgCompactNFData *str, StgWord aligned_size)
{
    StgCompactNFDataBlock *block;
    bdescr *bd;

    block = compactAllocateBlock(cap, aligned_size, NULL,
                                 compactGetFirstBlock(str),
                                 ALLOCATE_APPEND);
    block->owner = str;

    // The last data block always points to the first symbols block
    ASSERT (str->last->next == str->symbols);
    block->next = str->symbols;

    str->last->next = block;
    str->last = block;
    if (str->nursery == NULL)
        str->nursery = block;
    str->totalDataW += aligned_size / sizeof(StgWord);

    bd = Bdescr((P_)block);
    bd->free = (StgPtr)((W_)block + sizeof(StgCompactNFDataBlock));
    ASSERT (bd->free == (StgPtr)block + sizeofW(StgCompactNFDataBlock));

    str->totalW += bd->blocks * BLOCK_SIZE_W;

    return block;
}

void
compactResize (Capability *cap, StgCompactNFData *str, StgWord new_size)
{
    StgWord aligned_size;

    aligned_size = BLOCK_ROUND_UP(new_size + sizeof(StgCompactNFDataBlock));
    if (aligned_size >= BLOCK_SIZE * BLOCKS_PER_MBLOCK)
        aligned_size = BLOCK_SIZE * BLOCKS_PER_MBLOCK;

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
    StgCompactNFDataBlock *block;

    // try the nursery first
 retry:
    if (str->nursery != NULL) {
        if (allocate_in_compact(str->nursery, sizeW, at))
            return;

        if (block_is_full (str->nursery)) {
            str->nursery = str->nursery->next;
            goto retry;
        }

        // try subsequent blocks
        block = str->nursery->next;
        while (block != NULL) {
            if (allocate_in_compact(block, sizeW, at))
                return;

            block = block->next;
        }
    }

    block = compactAppendBlock(cap, str, str->autoBlockW * sizeof(StgWord));
    ASSERT (str->nursery != NULL);
    ok = allocate_in_compact(block, sizeW, at);
#if DEBUG
    ASSERT (ok);
#else
    // tell gcc that ok will be true at this point - this allows GCC
    // to infer that *at was properly initialized
    if (!ok)
        __builtin_unreachable();
#endif
}

static void *
slow_find_symbol (const void *address)
{
    int r;
    Dl_info info;
    int namelen;
    char *name;

    r = dladdr(address, &info);
    if (__builtin_expect(r < 0, 0)) {
        debugBelch("Failed to resolve info table at 0x%p", address);
        return NULL;
    }

    namelen = strlen(info.dli_sname);
    name = stgMallocBytes(namelen+1, "slow_find_symbol");
    memcpy(name, info.dli_sname, namelen+1);

    insertHashTable(own_symbol_table, (StgWord)address, name);

    return name;
}

static inline void
add_one_symbol (StgCompactNFData *str, StgClosure *q)
{
    const void *address = q->header.info;
    void *name = lookupHashTable(str->symbols_hash, (StgWord)address);

    if (__builtin_expect(name != NULL, 1))
        return;

    name = lookupHashTable(own_symbol_table, (StgWord)address);

    if (__builtin_expect(name == NULL, 0)) {
        name = slow_find_symbol(address);

        if (__builtin_expect(name == NULL, 0))
            return;
    }

    insertHashTable(str->symbols_hash, (StgWord)address, name);
    str->symbols_serial ++;
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

    if (str->symbols_hash != NULL)
        add_one_symbol(str, from);

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
simple_scavenge_mut_arr_ptrs (Capability       *cap,
                              StgCompactNFData *str,
                              HashTable        *hash,
                              StgMutArrPtrs    *a)
{
    StgPtr p, q;

    p = (StgPtr)&a->payload[0];
    q = (StgPtr)&a->payload[a->ptrs];
    for (; p < q; p++) {
        simple_evacuate(cap, str, hash, (StgClosure**)p);
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

        case ARR_WORDS:
            p += arr_words_sizeW((StgArrWords*)p);
            break;

        case MUT_ARR_PTRS_FROZEN:
        case MUT_ARR_PTRS_FROZEN0:
            simple_scavenge_mut_arr_ptrs(cap, str, hash, (StgMutArrPtrs*)p);
            p += mut_arr_ptrs_sizeW((StgMutArrPtrs*)p);
            break;

        case SMALL_MUT_ARR_PTRS_FROZEN:
        case SMALL_MUT_ARR_PTRS_FROZEN0:
        {
            StgPtr end;

            end = (P_)((StgClosure *)p)->payload +
                ((StgSmallMutArrPtrs*)p)->ptrs;
            for (p = (P_)((StgClosure *)p)->payload; p < end; p++) {
                simple_evacuate(cap, str, hash, (StgClosure **)p);
            }
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

    // Note: simple_scavenge_block can change str->last, which
    // changes this check, in addition to iterating through
    while (first_block != str->last) {
        first_block = first_block->next;
        simple_scavenge_block(cap, str, first_block, hash,
                              (P_)first_block + sizeofW(StgCompactNFDataBlock));
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
    case ARR_WORDS:
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
    case ARR_WORDS:
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

    // Other trickery: we pass NULL as first, which means our blocks
    // are always in generation 0
    // This is correct because the GC has never seen the blocks so
    // it had no chance of promoting them

    block = compactAllocateBlockAtInternal (cap, addr, aligned_size,
                                            ALLOCATE_IMPORT);

    if ((P_)block != addr && previous != NULL)
        previous->next = block;

    bd = Bdescr((P_)block);
    bd->free = (P_)((W_)bd->start + size);

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
    } while (block && block->owner);

    return rtsFalse;
}

STATIC_INLINE StgCompactNFDataBlock *
find_pointer(StgCompactNFData *str, StgClosure *q)
{
    StgCompactNFDataBlock *block;
    StgWord address = (W_)q;

    block = compactGetFirstBlock(str);
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
fixup_mut_arr_ptrs (StgCompactNFData *str,
                    StgMutArrPtrs    *a)
{
    StgPtr p, q;

    p = (StgPtr)&a->payload[0];
    q = (StgPtr)&a->payload[a->ptrs];
    for (; p < q; p++) {
        if (!fixup_one_pointer(str, (StgClosure**)p))
            return rtsFalse;
    }

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

        case ARR_WORDS:
            p += arr_words_sizeW((StgArrWords*)p);
            break;

        case MUT_ARR_PTRS_FROZEN:
        case MUT_ARR_PTRS_FROZEN0:
            fixup_mut_arr_ptrs(str, (StgMutArrPtrs*)p);
            p += mut_arr_ptrs_sizeW((StgMutArrPtrs*)p);
            break;

        case SMALL_MUT_ARR_PTRS_FROZEN:
        case SMALL_MUT_ARR_PTRS_FROZEN0:
        {
            StgPtr end;

            end = (P_)((StgClosure *)p)->payload +
                ((StgSmallMutArrPtrs*)p)->ptrs;
            for (p = (P_)((StgClosure *)p)->payload; p < end; p++) {
                if (!fixup_one_pointer(str, (StgClosure **)p))
                    return rtsFalse;
            }
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
            debugBelch("Invalid non-NFData closure (type %d) in Compact\n", info->type);
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
    } while(block && block->owner);

    return rtsTrue;
}

static void
fixup_early(StgCompactNFData *str, StgCompactNFDataBlock *block)
{
    StgCompactNFDataBlock *last;

    // This assignment is not needed but gcc complains without it
    // (it obviously cannot infer that at least a block in the chain
    // has block->owner != NULL)
    last = block;

    do {
        // Data blocks have owner != NULL, because they contain
        // GC-able objects, while symbols block have owner == NULL
        // (and in theory we should be able to share them sometimes,
        // even though we don't)
        // (we rely on the fact that NULL pointers on the origin
        // machine will stay as NULL pointers here, because we cannot
        // check str->last as we do in build_symbol_table_loop())
        // (the same block->owner == NULL check happens in fixup_loop()
        // and any_needs_fixup())
        if (block->owner != NULL) {
            last = block;
        }

        block = block->next;
    } while(block);

    str->last = last;

    str->symbols = str->last->next;
    str->symbols_hash = NULL;
    str->symbols_serial = 0;
}

static void
fixup_late(StgCompactNFData *str, StgCompactNFDataBlock *block)
{
    StgCompactNFDataBlock *nursery;
    bdescr *bd;
    StgWord totalW;
    StgWord totalDataW;

    nursery = block;
    totalW = 0;
    totalDataW = 0;
    do {
        block->self = block;

        bd = Bdescr((P_)block);
        totalW += bd->blocks * BLOCK_SIZE_W;

        if (block->owner != NULL) {
            if (bd->free != bd->start)
                nursery = block;
            block->owner = str;
            totalDataW += bd->blocks * BLOCK_SIZE_W;
        }

        block = block->next;
    } while(block);

    str->nursery = nursery;
    str->totalW = totalW;
    str->totalDataW = totalDataW;
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

static StgWord
symbol_table_sizeW(StgCompactNFData *str)
{
    StgCompactNFDataBlock *block;
    bdescr *bd;
    StgWord sizeW;

    block = str->symbols;
    sizeW = 0;
    while (block) {
        bd = Bdescr((P_)block);
        sizeW += bd->free - (bd->start + sizeofW(StgCompactNFDataBlock));
        block = block->next;
    }

    return sizeW;
}

static StgWord
alignup8(StgWord w) {
    return (w + 7) / 8 * 8;
}

static inline StgWord64 *
symbol_table_block_begin(StgCompactNFDataBlock *block)
{
    StgWord address;

    address = ((W_)block + sizeof(StgCompactNFDataBlock));
    if (sizeof(StgWord) != sizeof(StgWord64))
        address = alignup8(address);

    return (StgWord64*)address;
}

static StgCompactNFDataBlock *
find_symbol_block_for(StgCompactNFData *str, StgWord *word64_idx)
{
    StgCompactNFDataBlock *block;
    bdescr *bd;

    block = str->symbols;
    ASSERT (block != NULL);
    do {
        StgWord n_words64;

        bd = Bdescr((P_)block);
        n_words64 = bd->free - (P_)symbol_table_block_begin(block);
        n_words64 /= sizeof(StgWord64) / sizeof(StgWord);

        if (*word64_idx < n_words64)
            return block;

        *word64_idx -= n_words64;
        block = block->next;
    } while (block);

#ifdef DEBUG
    barf("index outside the last block");
#else
    __builtin_unreachable();
#endif
}

static void
symbol_item_at(StgCompactNFData  *str,
               StgWord            word64_idx,
               StgWord           *item_begin,
               StgWord           *item_end,
               StgWord64         *address,
               const char       **name)
{
    StgCompactNFDataBlock *block;
    StgWord64 *symbol_table;
    bdescr *bd;
    StgWord64 word;
    StgWord block_n_words;

    block = find_symbol_block_for(str, &word64_idx);

    bd = Bdescr((P_)block);
    symbol_table = symbol_table_block_begin(block);

    block_n_words = bd->free - (P_)symbol_table_block_begin(block);
    block_n_words /= sizeof(StgWord64) / sizeof(StgWord);

    ASSERT (word64_idx < block_n_words);

    // word64_idx points somewhere in the middle of
    // of a symbol item (which is made of 64 bits of address
    // followed by the symbol name, 0 padded to 64 bit alignment)
    // we need to find where symbol item starts, and to do
    // so we rely on one of the quirks of x86_64: the upper two
    // bytes of an address are always 0
    // if word64_idx points inside the name, at least the higher
    // byte will be not 0, because there cannot be NUL characters
    // in a string (there is no NUL terminator) and the higher
    // byte must for sure belong to the string and not to the
    // padding (this is why big endian is necessary)

    word = be64toh(symbol_table[word64_idx]);
    while (word >> 48 != 0ULL) {
        // If the first byte starts with 0, both must start with
        // 0, because it must be an address!
        // mask it out and see what happens...
        if (__builtin_expect(word >> 56 == 0ULL, 0)) {
            debugBelch("Corrupted symbol table imported (invalid address)");
            word &= 0xffff0000ffffffffULL;
            continue;
        }

        // At the very beginning of the symbol table
        // we must find an address
        if (__builtin_expect(word64_idx == 0, 0)) {
            debugBelch("Corrupted symbol table imported (invalid first entry)");
            break;
        }

        word64_idx --;
        word = be64toh(symbol_table[word64_idx]);
    }

    *item_begin = word64_idx;
    *address = word;
    *name = (const char*)(&symbol_table[word64_idx + 1]);
    word64_idx += 2;

    if (__builtin_expect(word64_idx > block_n_words, 0)) {
        debugBelch("Corrupted symbol table imported (address without name)");
        *item_end = word64_idx;
        return;
    }

    while (word64_idx < block_n_words) {
        word = be64toh(symbol_table[word64_idx]);

        if (word >> 48 == 0ULL)
            break;

        word64_idx ++;
    }

    *item_end = word64_idx;
}

static const char *
slow_path_lookup_symbol_name(StgCompactNFData *str, void *address)
{
    StgWord begin, end;
    StgWord sizeW, sizeW64;

    sizeW = symbol_table_sizeW(str);
    // 1 word64 means only the serial is present
    if (sizeW <= sizeofW(StgWord64))
        return NULL;

    sizeW64 = sizeW * sizeof(StgWord) / sizeof(StgWord64);
    begin = 1; // ignore the serial
    end = sizeW64;

    while (begin < end) {
        StgWord mid = (begin + end) / 2;
        StgWord item_begin;
        StgWord item_end;
        StgWord64 addr;
        const char *name;

        symbol_item_at(str, mid, &item_begin, &item_end, &addr, &name);

        if ((StgWord64)address == addr)
            return name;

        if ((StgWord64)address < addr) {
            end = item_begin;
        } else {
            begin = item_end;
        }
    }

    return NULL;
}

static void *
slow_path_lookup_symbol(StgCompactNFData *str, void *address)
{
    const char *name;

    name = slow_path_lookup_symbol_name(str, address);
    if (name == NULL)
        return NULL;

    return dlsym(RTLD_DEFAULT, name);
}

static void *
lookup_one_symbol(StgCompactNFData *str, HashTable *table, void *address)
{
    void *converted;

    converted = lookupHashTable(table, (StgWord)address);
    if (converted == NULL) {
        converted = slow_path_lookup_symbol(str, address);
        if (converted == NULL)
            return NULL;

        insertHashTable(table, (StgWord)address, converted);
    }

    return converted;
}

static rtsBool
fixup_one_info_table(StgCompactNFData *str, HashTable *table, StgClosure *q)
{
    void *address;
    void *converted;

    address = (void*) q->header.info;
    converted = lookup_one_symbol(str, table, address);
    if (converted == NULL)
        return rtsFalse;

    q->header.info = address;
    return rtsTrue;
}

static rtsBool
fixup_info_tables(StgCompactNFData *str, HashTable *table, StgCompactNFDataBlock *block)
{
    bdescr *bd;
    StgPtr p;
    StgClosure *q;

    bd = Bdescr((P_)block);
    p = (P_) firstBlockGetCompact(block);
    while (p < bd->free) {
        q = (StgClosure*)p;
        if (!fixup_one_info_table(str, table, q))
            return rtsFalse;
        p += closure_sizeW(q);
    }

    return rtsTrue;
}

static rtsBool
fixup_info_tables_loop(StgCompactNFData *str, StgCompactNFDataBlock *block)
{
    StgCompactNFDataBlock *next;
    HashTable *table;

    table = lookupHashTable(foreign_conversion_table, (StgWord)str->build_id);
    if (table == NULL) {
        table = allocHashTable();
        insertHashTable(foreign_conversion_table, (StgWord)str->build_id,
                        table);
    }

    next = block;
    do {
        block = next;
        if (!fixup_info_tables(str, table, block))
            return rtsFalse;

        next = block->next;
    } while (block != str->last);

    return rtsTrue;
}

static rtsBool
maybe_fixup_info_tables (StgCompactNFDataBlock *block,
                         StgCompactNFData      *str,
                         rtsBool                trust_info_tables)
{
    const StgWord8 *build_id;

    if (trust_info_tables)
        return rtsTrue;

    build_id = getBinaryBuildId();
    if (memcmp(str->build_id, build_id, BUILD_ID_SIZE) == 0)
        return rtsTrue;

    // Slow path: reconstruct info tables
    debugBelch("Binary versions do not match, slow path to adjust"
               " info tables\n");

    return fixup_info_tables_loop(str, block);
}

StgPtr
compactFixupPointers(StgCompactNFData *str,
                     StgClosure       *root,
                     StgInt            trust_info_tables)
{
    StgCompactNFDataBlock *block;
    bdescr *bd;
    StgWord total_blocks;

    block = compactGetFirstBlock(str);

    fixup_early(str, block);

    if (maybe_fixup_info_tables(block, str, trust_info_tables))
        root = maybe_fixup_internal_pointers(block, str, root);
    else
        root = NULL;

    // Do the late fixup even if we did not fixup all
    // internal pointers or info tables, we need that for GC and Sanity
    fixup_late(str, block);

    // Now we're ready to let the GC, Sanity, the profiler
    // etc. know about this object
    bd = Bdescr((P_)block);

    total_blocks = str->totalW / BLOCK_SIZE_W;

    ACQUIRE_SM_LOCK;
    ASSERT (bd->gen == g0);
    ASSERT (g0->n_compact_blocks_in_import >= total_blocks);
    g0->n_compact_blocks_in_import -= total_blocks;
    g0->n_compact_blocks += total_blocks;
    dbl_link_onto(bd, &g0->compact_objects);
    RELEASE_SM_LOCK;

    return (StgPtr)root;
}

static inline StgWord
symbols_get_serial (StgCompactNFDataBlock *block)
{
    return *(StgWord*)((W_)block + sizeof(StgCompactNFDataBlock));
}

static void
build_symbol_table(StgCompactNFData *str, StgCompactNFDataBlock *block)
{
    bdescr *bd;
    StgPtr p;
    StgClosure *q;

    bd = Bdescr((P_)block);
    p = (P_) firstBlockGetCompact(block);
    while (p < bd->free) {
        ASSERT (LOOKS_LIKE_CLOSURE_PTR(p));
        q = (StgClosure*)p;
        add_one_symbol(str, q);

        p += closure_sizeW(q);
    }
}

static void
build_symbol_table_loop(StgCompactNFData *str, StgCompactNFDataBlock *block)
{
    StgCompactNFDataBlock *next;

    next = block;
    do {
        block = next;
        build_symbol_table(str, block);

        next = block->next;
    } while (block != str->last);
}

typedef struct {
    StgWord  address;
    void    *name;
} SymbolTableItem;

static void
linearize_table(StgWord key, void *value, void *userdata)
{
    SymbolTableItem **p = userdata;
    SymbolTableItem *q;

    q = *p;
    q->address = key;
    q->name = value;

    *p = q + 1;
}

static int
symbol_compare(const void *v1, const void *v2)
{
    const SymbolTableItem *s1 = v1;
    const SymbolTableItem *s2 = v2;

    return s1->address - s2->address;
}

static void
serialize_symbol_table(Capability *cap, StgCompactNFData *str)
{
    StgWord64 *serial;
    StgCompactNFDataBlock *block;
    bdescr *bd;
    SymbolTableItem *array, *tmp;
    int array_size;
    int i;

    ASSERT (str->symbols == NULL);
    ASSERT (str->symbols_hash != NULL);

    block = compactAllocateBlock(cap, BLOCK_SIZE, NULL,
                                 compactGetFirstBlock(str),
                                 ALLOCATE_APPEND);
    block->owner = NULL;
    block->self = block;
    block->next = NULL;

    serial = symbol_table_block_begin(block);
    *serial = str->symbols_serial;

    bd = Bdescr((P_)block);
    bd->free = (P_)(serial + 1);

    str->symbols = block;
    str->last->next = block;
    str->totalW += bd->blocks * BLOCK_SIZE_W;

    array_size = keyCountHashTable(str->symbols_hash);
    // There is at least the info pointer for the StgCompactNFData
    ASSERT (array_size >= 1);
    array = stgMallocBytes(sizeof(SymbolTableItem) * array_size,
                           "serialize_symbol_table");
    tmp = array;
    forEachHashTable(str->symbols_hash, linearize_table, &tmp);

    qsort(array, array_size, sizeof(SymbolTableItem), symbol_compare);

    for (i = 0; i < array_size; i++) {
        SymbolTableItem *item = &array[i];
        int len = strlen(item->name);
        StgWord aligned_len = alignup8(len);
        StgWord reqd = sizeof(StgWord64) + aligned_len;
        StgWord64 *at;

        if ((W_)bd->free + reqd > (W_)bd->start + BLOCK_SIZE * bd->blocks) {
            StgCompactNFDataBlock *new_block;

            new_block = compactAllocateBlock(cap, BLOCK_SIZE, NULL,
                                             compactGetFirstBlock(str),
                                             ALLOCATE_APPEND);
            new_block->owner = NULL;
            new_block->self = block;
            new_block->next = NULL;

            block->next = new_block;
            block = new_block;
            bd = Bdescr((P_)block);
            bd->free = (P_)symbol_table_block_begin(block);

            str->totalW += bd->blocks * BLOCK_SIZE_W;
        }

        ASSERT ((W_)bd->free + reqd <= (W_)bd->start + BLOCK_SIZE * bd->blocks);

        at = (StgWord64*) bd->free;
        bd->free += reqd / sizeof(StgWord);

        // Storage format:
        // we write down the address in big endian
        // so that it starts with at least 2 zero bytes
        // (in x86_64, only 48 bits are addressable)
        // These two bytes are used as NUL terminator
        // for the previous item, and they are used to find
        // the beginning of a structure during the binary
        // search

        *at = htobe64(item->address);
        at++;

        // note: if len is a multiple of 8, there will not
        // be any 0 padding at the end, and thus no NUL
        // terminator for the name
        // we rely on the next item to find the name length

        memcpy(at, item->name, len);
        memset((char*)at + len, 0, aligned_len - len);
    }

    stgFree(array);
}

void
compactInitForSymbols(StgCompactNFData *str)
{
    if (str->symbols_hash != NULL)
        return;

    str->symbols_hash = allocHashTable();
    build_symbol_table_loop(str, compactGetFirstBlock(str));
}

void
compactBuildSymbolTable(Capability *cap, StgCompactNFData *str)
{
    // Check if no changes happened since last time
    if (str->symbols &&
        str->symbols_serial == symbols_get_serial(str->symbols))
        return;

    compactInitForSymbols(str);

    if (str->symbols != NULL) {
        ASSERT (str->last->next == str->symbols);
        freeBlockChain(str->symbols);
        str->symbols = NULL;
        str->last->next = NULL;
    }

    serialize_symbol_table(cap, str);
}
