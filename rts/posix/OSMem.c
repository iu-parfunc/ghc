/* -----------------------------------------------------------------------------
 *
 * (c) The University of Glasgow 2006-2007
 *
 * OS-specific memory management
 *
 * ---------------------------------------------------------------------------*/

// This is non-posix compliant.
// #include "PosixSource.h"

#include "Rts.h"

#include "RtsUtils.h"
#include "sm/OSMem.h"
#include "sm/HeapAlloc.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include <errno.h>

#if darwin_HOST_OS || ios_HOST_OS
#include <mach/mach.h>
#include <mach/vm_map.h>
#include <sys/sysctl.h>
#endif

static caddr_t next_request = 0;

void osMemInit(void)
{
    next_request = (caddr_t)RtsFlags.GcFlags.heapBase;
}

/* -----------------------------------------------------------------------------
   The mmap() method

   On Unix-like systems, we use mmap() to allocate our memory.  We
   want memory in chunks of MBLOCK_SIZE, and aligned on an MBLOCK_SIZE
   boundary.  The mmap() interface doesn't give us this level of
   control, so we have to use some heuristics.

   In the general case, if we want a block of n megablocks, then we
   allocate n+1 and trim off the slop from either side (using
   munmap()) to get an aligned chunk of size n.  However, the next
   time we'll try to allocate directly after the previously allocated
   chunk, on the grounds that this is aligned and likely to be free.
   If it turns out that we were wrong, we have to munmap() and try
   again using the general method.

   Note on posix_memalign(): this interface is available on recent
   systems and appears to provide exactly what we want.  However, it
   turns out not to be as good as our mmap() implementation, because
   it wastes extra space (using double the address space, in a test on
   x86_64/Linux).  The problem seems to be that posix_memalign()
   returns memory that can be free()'d, so the library must store
   extra information along with the allocated block, thus messing up
   the alignment.  Hence, we don't use posix_memalign() for now.

   -------------------------------------------------------------------------- */

/*
 A wrapper around mmap(), to abstract away from OS differences in
 the mmap() interface.

 It supports the following operations:
 - reserve: find a new chunk of available address space, and make it so
            that we own it (no other library will get it), but don't actually
            allocate memory for it
            the addr is a hint for where to place the memory (and most
            of the time the OS happily ignores!)
 - commit: given a chunk of address space that we know we own, make sure
           there is some memory backing it
           the addr is not a hint, it must point into previously reserved
           address space, or bad things happen
 - reserve&commit: do both at the same time

 The naming is chosen from the Win32 API (VirtualAlloc) which does the
 same thing and has done so forever, while support for this in Unix systems
 has only been added recently and is hidden in the posix portability mess.
 It is confusing because to get the reserve behavior we need MAP_NORESERVE
 (which tells the kernel not to allocate backing space), but heh...
*/
enum
{
    MEM_RESERVE = 1,
    MEM_COMMIT = 2,
    MEM_RESERVE_AND_COMMIT = MEM_RESERVE | MEM_COMMIT
};

static void *
my_mmap (void *addr, W_ size, int operation)
{
    void *ret;

#if darwin_HOST_OS
    // Without MAP_FIXED, Apple's mmap ignores addr.
    // With MAP_FIXED, it overwrites already mapped regions, whic
    // mmap(0, ... MAP_FIXED ...) is worst of all: It unmaps the program text
    // and replaces it with zeroes, causing instant death.
    // This behaviour seems to be conformant with IEEE Std 1003.1-2001.
    // Let's just use the underlying Mach Microkernel calls directly,
    // they're much nicer.

    kern_return_t err = 0;
    ret = addr;

    if(operation & MEM_RESERVE)
    {
        if(addr)    // try to allocate at address
            err = vm_allocate(mach_task_self(),(vm_address_t*) &ret,
                              size, FALSE);
        if(!addr || err)    // try to allocate anywhere
            err = vm_allocate(mach_task_self(),(vm_address_t*) &ret,
                              size, TRUE);
    }

    if(err) {
        // don't know what the error codes mean exactly, assume it's
        // not our problem though.
        errorBelch("memory allocation failed (requested %" FMT_Word " bytes)",
                   size);
        stg_exit(EXIT_FAILURE);
    }

    if(operation & MEM_COMMIT) {
        vm_protect(mach_task_self(), (vm_address_t)ret, size, FALSE,
                   VM_PROT_READ|VM_PROT_WRITE);
    }

#else

    int prot, flags;
    if (operation & MEM_COMMIT)
        prot = PROT_READ | PROT_WRITE;
    else
        prot = PROT_NONE;
    if (operation == MEM_RESERVE)
        flags = MAP_NORESERVE;
    else if (operation == MEM_COMMIT)
        flags = MAP_FIXED;
    else
        flags = 0;

#if defined(irix_HOST_OS)
    {
        if (operation & MEM_RESERVE)
        {
            int fd = open("/dev/zero",O_RDONLY);
            ret = mmap(addr, size, prot, flags | MAP_PRIVATE, fd, 0);
            close(fd);
        }
        else
        {
            ret = mmap(addr, size, prot, flags | MAP_PRIVATE, -1, 0);
        }
    }
#elif hpux_HOST_OS
    ret = mmap(addr, size, prot, flags | MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
#elif linux_HOST_OS
    ret = mmap(addr, size, prot, flags | MAP_ANON | MAP_PRIVATE, -1, 0);
    if (ret == (void *)-1 && errno == EPERM) {
        // Linux may return EPERM if it tried to give us
        // a chunk of address space below mmap_min_addr,
        // See Trac #7500.
        if (addr != 0 && (operation & MEM_RESERVE)) {
            // Try again with no hint address.
            // It's not clear that this can ever actually help,
            // but since our alternative is to abort, we may as well try.
            ret = mmap(0, size, prot, flags | MAP_ANON | MAP_PRIVATE, -1, 0);
        }
        if (ret == (void *)-1 && errno == EPERM) {
            // Linux is not willing to give us any mapping,
            // so treat this as an out-of-memory condition
            // (really out of virtual address space).
            errno = ENOMEM;
        }
    }
#else
    ret = mmap(addr, size, prot, flags | MAP_ANON | MAP_PRIVATE, -1, 0);
#endif
#endif

    if (ret == (void *)-1) {
        if (errno == ENOMEM ||
            (errno == EINVAL && sizeof(void*)==4 && size >= 0xc0000000)) {
            // If we request more than 3Gig, then we get EINVAL
            // instead of ENOMEM (at least on Linux).
            errorBelch("out of memory (requested %" FMT_Word " bytes)", size);
            stg_exit(EXIT_FAILURE);
        } else {
            barf("getMBlock: mmap: %s", strerror(errno));
        }
    }

    return ret;
}

// Implements the general case: allocate a chunk of memory of 'size'
// mblocks.

static void *
gen_map_mblocks (W_ size)
{
    int slop;
    StgWord8 *ret;

    // Try to map a larger block, and take the aligned portion from
    // it (unmap the rest).
    size += MBLOCK_SIZE;
    ret = my_mmap(0, size, MEM_RESERVE_AND_COMMIT);

    // unmap the slop bits around the chunk we allocated
    slop = (W_)ret & MBLOCK_MASK;

    if (munmap((void*)ret, MBLOCK_SIZE - slop) == -1) {
      barf("gen_map_mblocks: munmap failed");
    }
    if (slop > 0 && munmap((void*)(ret+size-slop), slop) == -1) {
      barf("gen_map_mblocks: munmap failed");
    }

    // ToDo: if we happened to get an aligned block, then don't
    // unmap the excess, just use it. For this to work, you
    // need to keep in mind the following:
    //     * Calling my_mmap() with an 'addr' arg pointing to
    //       already my_mmap()ed space is OK and won't fail.
    //     * If my_mmap() can't satisfy the request at the
    //       given 'next_request' address in getMBlocks(), that
    //       you unmap the extra mblock mmap()ed here (or simply
    //       satisfy yourself that the slop introduced isn't worth
    //       salvaging.)
    //

    // next time, try after the block we just got.
    ret += MBLOCK_SIZE - slop;
    return ret;
}

void *
osGetMBlocks(nat n)
{
  caddr_t ret;
  W_ size = MBLOCK_SIZE * (W_)n;

  if (next_request == 0) {
      // use gen_map_mblocks the first time.
      ret = gen_map_mblocks(size);
  } else {
      ret = my_mmap(next_request, size, MEM_RESERVE_AND_COMMIT);

      if (((W_)ret & MBLOCK_MASK) != 0) {
          // misaligned block!
#if 0 // defined(DEBUG)
          errorBelch("warning: getMBlock: misaligned block %p returned "
                     "when allocating %d megablock(s) at %p",
                     ret, n, next_request);
#endif

          // unmap this block...
          if (munmap(ret, size) == -1) {
              barf("getMBlock: munmap failed");
          }
          // and do it the hard way
          ret = gen_map_mblocks(size);
      }
  }
  // Next time, we'll try to allocate right after the block we just got.
  // ToDo: check that we haven't already grabbed the memory at next_request
  next_request = ret + size;

  return ret;
}

void osFreeMBlocks(char *addr, nat n)
{
    munmap(addr, n * MBLOCK_SIZE);
}

void osReleaseFreeMemory(void) {
    /* Nothing to do on POSIX */
}

void osFreeAllMBlocks(void)
{
    void *mblock;

    for (mblock = getFirstMBlock();
         mblock != NULL;
         mblock = getNextMBlock(mblock)) {
        munmap(mblock, MBLOCK_SIZE);
    }
}

W_ getPageSize (void)
{
    static W_ pageSize = 0;
    if (pageSize) {
        return pageSize;
    } else {
        long ret;
        ret = sysconf(_SC_PAGESIZE);
        if (ret == -1) {
           barf("getPageSize: cannot get page size");
        }
        pageSize = ret;
        return ret;
    }
}

/* Returns 0 if physical memory size cannot be identified */
StgWord64 getPhysicalMemorySize (void)
{
    static StgWord64 physMemSize = 0;
    if (!physMemSize) {
#if defined(darwin_HOST_OS) || defined(ios_HOST_OS)
        /* So, darwin doesn't support _SC_PHYS_PAGES, but it does
           support getting the raw memory size in bytes through
           sysctlbyname(hw.memsize); */
        size_t len = sizeof(physMemSize);
        int ret = -1;

        /* Note hw.memsize is in bytes, so no need to multiply by page size. */
        ret = sysctlbyname("hw.memsize", &physMemSize, &len, NULL, 0);
        if (ret == -1) {
            physMemSize = 0;
            return 0;
        }
#else
        /* We'll politely assume we have a system supporting _SC_PHYS_PAGES
         * otherwise.  */
        W_ pageSize = getPageSize();
        long ret = sysconf(_SC_PHYS_PAGES);
        if (ret == -1) {
#if defined(DEBUG)
            errorBelch("warning: getPhysicalMemorySize: cannot get "
                       "physical memory size");
#endif
            return 0;
        }
        physMemSize = ret * pageSize;
#endif /* darwin_HOST_OS */
    }
    return physMemSize;
}

void setExecutable (void *p, W_ len, rtsBool exec)
{
    StgWord pageSize = getPageSize();

    /* malloced memory isn't executable by default on OpenBSD */
    StgWord mask             = ~(pageSize - 1);
    StgWord startOfFirstPage = ((StgWord)p          ) & mask;
    StgWord startOfLastPage  = ((StgWord)p + len - 1) & mask;
    StgWord size             = startOfLastPage - startOfFirstPage + pageSize;
    if (mprotect((void*)startOfFirstPage, (size_t)size,
                 (exec ? PROT_EXEC : 0) | PROT_READ | PROT_WRITE) != 0) {
        barf("setExecutable: failed to protect 0x%p\n", p);
    }
}

#ifdef USE_LARGE_ADDRESS_SPACE


#ifdef USE_STRIPED_ALLOCATOR
void *osTryReserveHeapMemory(void *hint)
{
    void *p;

    (void)hint;

    /* We want to allocate exactly at the given address - no ifs, not buts */
    p = mmap((void*)MBLOCK_SPACE_BEGIN, MBLOCK_SPACE_SIZE, PROT_NONE,
             MAP_NORESERVE | MAP_FIXED | MAP_ANON | MAP_PRIVATE, -1, 0);
    if (p != (void*)MBLOCK_SPACE_BEGIN) { // covers NULL and MAP_FAILED (-1)
        barf("Failed to reserve address space");
    }

    return p;
}
#else
void *osTryReserveHeapMemory(void *hint)
{
    void *base, *top;
    void *start, *end;

    /* We try to allocate MBLOCK_SPACE_SIZE + MBLOCK_SIZE,
       because we need memory which is MBLOCK_SIZE aligned,
       and then we discard what we don't need */

    base = my_mmap(hint, MBLOCK_SPACE_SIZE + MBLOCK_SIZE, MEM_RESERVE);
    top = (void*)((W_)base + MBLOCK_SPACE_SIZE + MBLOCK_SIZE);

    if (((W_)base & MBLOCK_MASK) != 0) {
        start = MBLOCK_ROUND_UP(base);
        end = MBLOCK_ROUND_DOWN(top);
        ASSERT(((W_)end - (W_)start) == MBLOCK_SPACE_SIZE);

        if (munmap(base, (W_)start-(W_)base) < 0) {
            sysErrorBelch("unable to release slop before heap");
        }
        if (munmap(end, (W_)top-(W_)end) < 0) {
            sysErrorBelch("unable to release slop after heap");
        }
    } else {
        start = base;
    }

    return start;
}
#endif

void *osReserveHeapMemory(void)
{
    int attempt;
    void *at;

    /* We want to ensure the heap starts at least 8 GB inside the address space,
       to make sure that any dynamically loaded code will be close enough to the
       original code so that short relocations will work. This is in particular
       important on Darwin/Mach-O, because object files not compiled as shared
       libraries are position independent but cannot be loaded about 4GB.

       We do so with a hint to the mmap, and we verify the OS satisfied our
       hint. We loop a few times in case there is already something allocated
       there, but we bail if we cannot allocate at all.
    */

    attempt = 0;
    do {
        at = osTryReserveHeapMemory((void*)((W_)8 * (1 << 30) +
                                            attempt * BLOCK_SIZE));
    } while ((W_)at < ((W_)8 * (1 << 30)));

    return at;
}

void osCommitMemory(void *at, W_ size)
{
    my_mmap(at, size, MEM_COMMIT);
}

void osDecommitMemory(void *at, W_ size)
{
    int r;

    // First make the memory unaccessible (so that we get a segfault
    // at the next attempt to touch it)
    // We only do this in DEBUG because it forces the OS to remove
    // all MMU entries for this page range, and there is no reason
    // to do so unless there is memory pressure
#ifdef DEBUG
    r = mprotect(at, size, PROT_NONE);
    if(r < 0)
        sysErrorBelch("unable to make released memory unaccessible");
#endif

#ifdef MADV_FREE
    // Try MADV_FREE first, FreeBSD has both and MADV_DONTNEED
    // just swaps memory out
    r = madvise(at, size, MADV_FREE);
#else
    r = madvise(at, size, MADV_DONTNEED);
#endif
    if(r < 0)
        sysErrorBelch("unable to decommit memory");
}

void osReleaseHeapMemory(void)
{
    int r;

#ifdef USE_STRIPED_ALLOCATOR
    r = munmap((void*)MBLOCK_SPACE_BEGIN, MBLOCK_SPACE_SIZE);
#else
    r = munmap((void*)mblock_address_space_begin, MBLOCK_SPACE_SIZE);
#endif
    if(r < 0)
        sysErrorBelch("unable to release address space");
}

#endif
