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
#include "CNF.h"

static StgCompactNFData *
compactAllocate(Capability *cap, StgWord aligned_size)
{
    StgCompactNFData *self;
    bdescr *block;
    nat n_blocks;

    n_blocks = aligned_size / BLOCK_SIZE;
    self = (StgCompactNFData*) allocate(cap, aligned_size / sizeof(StgWord));

    SET_INFO((StgClosure*)self, &stg_COMPACT_NFDATA_info);
    for (block = Bdescr((P_)self); n_blocks > 0; block++, n_blocks--)
        block->flags |= BF_COMPACT;

    return self;
}

StgCompactNFData *
compactNew (Capability *cap, StgWord size)
{
    StgWord aligned_size;
    StgCompactNFData *self;

    aligned_size = BLOCK_ROUND_UP(size + sizeof(StgCompactNFData));
    self = compactAllocate(cap, aligned_size);

    self->allocatedW = aligned_size / sizeof(StgWord);
    self->free = (StgPtr)((W_)self + sizeof(StgCompactNFData));
    ASSERT (self->free == (StgPtr)self + sizeofW(StgCompactNFData));

    return self;
}

StgCompactNFData *
compactResize (Capability *cap, StgCompactNFData *str, StgWord new_size)
{
    StgWord current_size, aligned_size;
    StgCompactNFData *self;

    aligned_size = BLOCK_ROUND_UP(new_size + sizeof(StgCompactNFData));
    current_size = str->allocatedW * sizeof(StgWord);

    // It might be that new_size was still covered by the alignment
    // during the initial allocation (or maybe the user is asking to
    // shrink? we don't do that)
    if (aligned_size <= current_size)
        return str;

    self = compactAllocate(cap, aligned_size);

    memcpy (self, str, str->allocatedW * sizeof(StgWord));

    self->allocatedW = aligned_size / sizeof(StgWord);
    self->free = (StgPtr)((W_)str->free - (W_)str + (W_)self);

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
   them inside the compact and scavenge them normally.

   Note that if we allowed thunks and lazy evaluation the compact
   would be a mutable object, which would create all sorts of
   GC problems (besides, evaluating a thunk could exaust the
   compact space or yield an invalid object, and we would have
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
   "back" pointer: before each object in the compact space, we write
   a pointer to the original one. During the unforward pass, we
   scavenge the to space and then use this pointer to find the object
   in the outside space, where we restore a good info table taken
   from the to object.

   You might be tempted to replace the objects with StdInd to
   the object in the compact, but you would be wrong: the haskell
   code assumes that objects in the heap only become more evaluated
   (thunks to blackholes to inds to actual objects), and in
   particular it assumes that if a pointer is tagged the object
   is directly referenced and the values can be read directly,
   without entering the closure.

   FIXME: any better idea?
   FIXME: threading issues with concurrent access to the objects?
*/

static void
unroll_memcpy(StgPtr to, StgPtr from, StgWord size)
{
    for (; size > 0; size--)
        *(to++) = *(from++);
}

static rtsBool
copy_tag (StgCompactNFData *str, StgClosure **p, StgClosure *from, StgWord tag)
{
    StgPtr to;
    StgWord sizeW;

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
object_in_compact (StgCompactNFData *str, StgClosure *p)
{
    return ((P_)p >= (P_)str && (P_)p < str->free);
}

static rtsBool
simple_evacuate (StgCompactNFData *str, StgClosure **p)
{
    StgWord tag;
    StgClosure *from;

    from = *p;
    tag = GET_CLOSURE_TAG(from);
    from = UNTAG_CLOSURE(from);

    // Do nothing on static closures, just reference them.
    // They would not be GCed anyway. And in practice we should
    // not see them (because they are THUNKs or FUNs), but
    // we could see CHARLIKE and INTLIKE
    if (!HEAP_ALLOCED(from))
        return rtsTrue;

    // If the object referenced is already in the compact
    // (for example by reappending an object that was obtained
    // by compactGetRoot) then do nothing
    if (object_in_compact(str, from))
        return rtsTrue;

    if (IS_FORWARDING_PTR(from->header.info)) {
        StgClosure *c;

        c = (StgClosure*)UN_FORWARDING_PTR(from->header.info);
        *p = TAG_CLOSURE(tag, c);
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
        return simple_evacuate(str, p);

    case IND:
        // follow chains of indirections, don't evacuate them
        from = ((StgInd*)from)->indirectee;
        *p = from;
        // Evac.c uses a goto, but let's rely on a smart compiler
        // and get readable code instead
        return simple_evacuate(str, p);

    default:
        return copy_tag(str, p, from, tag);
    }
}

STATIC_INLINE rtsBool
unforward (StgCompactNFData *str, StgClosure **p)
{
    StgClosure *to;
    StgClosure *from;

    // In this pass, p points into the to space, not the from space!
    to = *p;
    to = UNTAG_CLOSURE(to);

    // Do nothing if the pointer was not rewritten into the to space
    // (can happen if the object is static, or if appending failed)
    if (!object_in_compact(str, to))
        return rtsTrue;

    ASSERT ((Bdescr((StgPtr)to)->flags & BF_COMPACT) != 0);

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
simple_evac_or_unforward (StgCompactNFData *str, StgClosure **p, rtsBool evac)
{
    if (evac)
        return simple_evacuate(str, p);
    else
        return unforward(str, p);
}

static rtsBool
simple_scavenge (StgCompactNFData *str, StgPtr p, rtsBool evac)
{
    StgInfoTable *info;

    while (p < str->free) {
        // Skip the back pointer added by evacuate
        ASSERT (LOOKS_LIKE_CLOSURE_PTR(*(StgClosure**)p));
        p++;

        ASSERT (LOOKS_LIKE_CLOSURE_PTR(p));
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

        case IND:
        case BLACKHOLE:
            if (!simple_evac_or_unforward(str, &((StgInd*)p)->indirectee, evac))
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

StgPtr
compactAppend (StgCompactNFData *str, StgClosure *what)
{
    rtsBool ok;
    StgClosure *root;
    StgClosure *tagged_root;
    StgPtr start;

    start = str->free;
    tagged_root = what;
    if (!simple_evacuate(str, &tagged_root))
        return NULL;

    root = UNTAG_CLOSURE(tagged_root);
    ASSERT((P_)root == (start+1) ||
           (!HEAP_ALLOCED(root) && root == UNTAG_CLOSURE(what)));

    ok = simple_scavenge(str, start, rtsTrue);

    // unforward what (manually because we don't need nasty
    // tricks for this one, and we don't need to check if
    // it is a forwarding pointer because we know it is)
    //
    // (but only if was actually copied into the compact,
    // to catch the static closure case)
    if ((P_)root == (start+1))
        UNTAG_CLOSURE(what)->header.info = root->header.info;
    simple_scavenge(str, start, rtsFalse);

    // Return the tagged pointer for outside use, or NULL
    // if failed
    if (ok) {
        return (StgPtr)tagged_root;
    } else {
        // Undo any partial allocation
        str->free = start;
        return NULL;
    }
}
