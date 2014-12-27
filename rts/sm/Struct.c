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

#include "GC.h"
#include "Struct.h"

static StgNFDataStruct *
structAllocate(Capability *cap, StgWord aligned_size)
{
    StgNFDataStruct *self;
    bdescr *block;
    nat n_blocks;

    n_blocks = aligned_size / BLOCK_SIZE;
    self = (StgNFDataStruct*) allocate(cap, aligned_size / sizeof(StgWord));

    for (block = Bdescr((P_)self); n_blocks > 0; block++, n_blocks--)
        block->flags |= BF_STRUCT;

    return self;
}

StgNFDataStruct *
structNew (Capability *cap, StgWord size)
{
    StgWord aligned_size;
    StgNFDataStruct *self;

    aligned_size = BLOCK_ROUND_UP(size + sizeof(StgNFDataStruct));
    self = structAllocate(cap, aligned_size);

    self->allocatedW = aligned_size / sizeof(StgWord);
    self->root = NULL;
    self->free = (StgPtr)((W_)self + sizeof(StgNFDataStruct));
    ASSERT (self->free == (StgPtr)self + sizeofW(StgNFDataStruct));

    return self;
}

StgNFDataStruct *
structResize (Capability *cap, StgNFDataStruct *str, StgWord new_size)
{
    StgWord current_size, aligned_size;
    StgNFDataStruct *self;

    aligned_size = BLOCK_ROUND_UP(new_size + sizeof(StgNFDataStruct));
    current_size = str->allocatedW * sizeof(StgWord);

    // It might be that new_size was still covered by the alignment
    // during the initial allocation (or maybe the user is asking to
    // shrink? we don't do that)
    if (aligned_size <= current_size)
        return str;

    self = structAllocate(cap, aligned_size);

    memcpy (self, str, str->allocatedW * sizeof(StgWord));

    self->allocatedW = aligned_size / sizeof(StgWord);
    self->free = (StgPtr)((W_)str->free - (W_)str + (W_)self);
    self->root = (StgClosure*)((W_)str->root - (W_)str + (W_)self);

    return self;
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
   them inside the struct and scavenge them normally.

   Note that if we allowed thunks and lazy evaluation the struct
   would be a mutable object, which would create all sorts of
   GC problems (besides, evaluating a thunk could exaust the
   struct space or yield an invalid object, and we would have
   no way to signal that to the user)

   Just like the real evacuate/scavenge pairs, we need to handle
   object loops. We want to use the same strategy of rewriting objects
   with forwarding pointer, but in a real GC, at the end the
   blocks from the old space are dropped (dropping all forwarding
   pointers at the same time), which we can't do here as we don't
   know all pointers to the objects being evacuate. We would need
   to extend the running code with knowledge of forwarding pointers
   (which would slow it down considerably) or run a major GC pass
   at the end (which is also slow).

   So instead we just run another scavenge pass that replaces
   forwarding pointers with good closures again. To do so, we need
   to find where are the original objects, which we do with a
   "back" pointer: before each object in the struct space, we write
   a pointer to the original one. During the unforward pass, we
   scavenge the to space and then use this pointer to find the object
   in the outside space, where we restore a good info table taken
   from the to object.

   FIXME: any better idea?
*/

static void
unroll_memcpy(StgPtr to, StgPtr from, StgWord size)
{
    for (; size > 0; size--)
        *(to++) = *(from++);
}

static rtsBool
simple_evacuate (StgNFDataStruct *str, StgClosure **p)
{
    StgWord sizeW;
    StgWord tag;
    StgClosure *from;
    StgPtr to;

    from = *p;
    tag = GET_CLOSURE_TAG(from);
    from = UNTAG_CLOSURE(from);

    // Do nothing on static closures, just reference them.
    // They would not be GCed anyway. And in practice we should
    // not see them (because they are THUNKs or FUNs), but
    // we could see CHARLIKE and INTLIKE
    if (!HEAP_ALLOCED(from))
        return rtsTrue;

    if (IS_FORWARDING_PTR(from->header.info)) {
        StgClosure *c;

        c = (StgClosure*)UN_FORWARDING_PTR(from->header.info);
        *p = TAG_CLOSURE(tag, c);
        return rtsTrue;
    }

    sizeW = 1 + closure_sizeW(from);
    to = str->free;

    if (to + sizeW >= ((StgPtr)str) + str->allocatedW)
        return rtsFalse;

    str->free += sizeW;

    // Store the original pointer so that we can unforward it
    // later
    *to = (StgWord)from;
    to++;
    sizeW--;

    // unroll memcpy for small sizes because we can
    // benefit of known alignment
    // (32 extracted from my magic hat)
    if (sizeW < 32)
        unroll_memcpy(to, (StgPtr)from, sizeW);
    else
        memcpy(to, from, sizeW * sizeof(StgWord));

    from->header.info = (const StgInfoTable *)MK_FORWARDING_PTR(to);
    *p = TAG_CLOSURE(tag, (StgClosure*)to);

    return rtsTrue;
}

STATIC_INLINE rtsBool
unforward (StgClosure **p)
{
    StgClosure *to;
    StgClosure *from;

    // In this pass, p points into the to space, not the from space!
    to = *p;
    to = UNTAG_CLOSURE(to);

    // Do nothing if the pointer was not rewritten into the to space
    if (!HEAP_ALLOCED(to))
        return rtsTrue;

    ASSERT ((Bdescr((StgPtr)to)->flags & BF_STRUCT) != 0);

    // Extract the from object looking before the to object
    from = (StgClosure*)(*(((StgPtr)to)-1));

    // It might not be a forwarding pointer because
    // it might be already unforwarded by another pointer to this
    // object
    if (IS_FORWARDING_PTR(from->header.info))
        from->header.info = to->header.info;

    return rtsTrue;
}

STATIC_INLINE rtsBool
simple_evac_or_unforward (StgNFDataStruct *str, StgClosure **p, rtsBool evac)
{
    if (evac)
        return simple_evacuate(str, p);
    else
        return unforward(p);
}

static rtsBool
simple_scavenge (StgNFDataStruct *str, StgPtr p, rtsBool evac)
{
    StgInfoTable *info;

    while (p < str->free) {
        info = get_itbl((StgClosure*)p);

        switch (info->type) {
        case CONSTR_1_0:
            if (!simple_evac_or_unforward(str, &((StgClosure*)p)->payload[0], evac))
                return rtsFalse;
        case CONSTR_0_1:
            p += sizeofW(StgClosure) + 1;
            break;

        case CONSTR_2_0:
            if (!simple_evac_or_unforward(str, &((StgClosure*)p)->payload[1], evac))
                return rtsFalse;
        case CONSTR_1_1:
            if (!simple_evac_or_unforward(str, &((StgClosure*)p)->payload[0], evac))
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
                if (!simple_evac_or_unforward(str, (StgClosure **)p, evac))
                    return rtsFalse;
            }
            p += info->layout.payload.nptrs;
            break;
        }

        default:
            debugBelch("Invalid non-NFData closure in Struct");
            return rtsFalse;
        }
    }

    return rtsTrue;
}

rtsBool
structAppend (StgNFDataStruct *str, StgClosure *what)
{
    rtsBool ok;
    StgPtr start;
    StgClosure *where;

    start = str->free;
    where = what;
    if (!simple_evacuate(str, &where))
        return rtsFalse;

    ok = simple_scavenge(str, start, rtsTrue);

    // unforward what (manually because we don't need nasty
    // tricks for this one, and we don't need to check if
    // it is a forwarding pointer because we know it is)
    //
    // Don't use where because that might be a tagged pointer
    what->header.info = ((StgClosure*)start)->header.info;
    simple_scavenge(str, start, rtsFalse);

    // We store the tagged pointer in the root field
    if (ok)
        str->root = where;

    return ok;
}
