/* -----------------------------------------------------------------------------
 *
 * (c) The GHC Team 1998-2014
 *
 * Computing the Build-ID of a running Haskell binary.
 *
 * ---------------------------------------------------------------------------*/

#define _GNU_SOURCE
#include "PosixSource.h"
#include <string.h>
#include "Rts.h"
#include "RtsUtils.h"

#include "rts/Md5.h"
#include "BuildId.h"

#if defined(linux_HOST_OS) || defined(solaris2_HOST_OS) || defined(freebsd_HOST_OS) || defined(kfreebsdgnu_HOST_OS) || defined(dragonfly_HOST_OS) || defined(netbsd_HOST_OS) || defined(openbsd_HOST_OS) || defined(gnu_HOST_OS)
# define OBJFORMAT_ELF
#include <elf.h>
#include <link.h>
#endif

static StgWord8 hashed_build_id[BUILD_ID_SIZE];
static rtsBool build_id_computed;

#define roundup(x, a) ((((W_)(x)) + ((a)-1)) & ~((a)-1))

#ifdef OBJFORMAT_ELF
static const StgWord8 *
find_build_id_library(struct dl_phdr_info *info,
                      StgWord             *build_id_size)
{
    ElfW(Half) i;

    for (i = 0; i < info->dlpi_phnum; i++) {
        const ElfW(Phdr) *phdr;
        ElfW(Word) size;
        const ElfW(Nhdr) *note;

        phdr = &info->dlpi_phdr[i];

        if (phdr->p_type != PT_NOTE)
            continue;

        size = phdr->p_memsz;
        note = (const ElfW(Nhdr)*)((W_)phdr->p_vaddr + (W_)info->dlpi_addr);
        while (size > 0) {
            StgWord name_pad, descriptor_pad;
            StgWord advance;
            const char *name;
            const StgWord8 *descriptor;

            // The descriptor is supposed to be aligned to a natural
            // boundary (4 or 8 bytes), according to the Solaris ELF
            // documentation
            // but in practice it is always aligned to 4 bytes on GNU
            // (According to readelf sources, the exception is ia64 VMS,
            // which is not interesting here)

            name = (const char*)((W_)note + sizeof(ElfW(Nhdr)));
            name_pad = roundup(note->n_namesz, 4) - note->n_namesz;

            descriptor = (const StgWord8*)((W_)name + note->n_namesz + name_pad);
            descriptor_pad = roundup(note->n_descsz, 4) - note->n_descsz;

            if (note->n_namesz == 4 &&
                name[0] == 'G' && name[1] == 'N' && name[2] == 'U' &&
                name[3] == 0 &&
                note->n_type == NT_GNU_BUILD_ID) {
                *build_id_size = note->n_descsz;
                return descriptor;
            }

            advance = sizeof(ElfW(Nhdr)) + note->n_namesz + note->n_descsz
                + name_pad + descriptor_pad;
            size -= advance;
            note = (const ElfW(Nhdr)*)((W_)note + advance);
        }
    }

    errorBelch("Could not find .note.gnu.build-id section in"
               " library %s", info->dlpi_name);
    return NULL;
}

static int
compute_build_id_library(struct dl_phdr_info *info,
                         size_t               info_size STG_UNUSED,
                         void                *user_data)
{
    struct StgMD5Context *ctx = user_data;
    const StgWord8 *build_id;
    StgWord size;

    build_id = find_build_id_library(info, &size);
    if (build_id != NULL)
        stg_MD5Update(ctx, build_id, size);

    return 0;
}
#endif

static void
computeBuildId(void)
{
    struct StgMD5Context ctx;

    stg_MD5Init(&ctx);

#ifdef OBJFORMAT_ELF
    dl_iterate_phdr(compute_build_id_library, &ctx);
#else
# warning "cannot compute the build id of this object"
#endif

    stg_MD5Final(hashed_build_id, &ctx);
    build_id_computed = rtsTrue;
}

const StgWord8 *
getBinaryBuildId(void)
{
    if (!build_id_computed)
        computeBuildId();

    return hashed_build_id;
}
