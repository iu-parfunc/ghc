/* -----------------------------------------------------------------------------
 *
 * (c) The GHC Team 1998-2014
 *
 * Computing the Build-ID of a running Haskell binary.
 *
 * ---------------------------------------------------------------------------*/

#ifndef BUILD_ID_H
#define BUILD_ID_H

#include "BeginPrivate.h"

// Size of a md5 hash
#define BUILD_ID_SIZE 16

const StgWord8 *getBinaryBuildId(void);

#include "EndPrivate.h"

#endif
