/*
 * This code implements the MD5 message-digest algorithm.
 * The algorithm is due to Ron Rivest.  This code was
 * written by Colin Plumb in 1993, no copyright is claimed.
 * This code is in the public domain; do with it what you wish.
 *
 * Equivalent code is available from RSA Data Security, Inc.
 * This code has been tested against that, and is equivalent,
 * except that you don't need to include two pages of legalese
 * with every copy.
 *
 * To compute the message digest of a chunk of bytes, declare an
 * MD5Context structure, pass it to MD5Init, call MD5Update as
 * needed on buffers full of bytes, and then call MD5Final, which
 * will fill a supplied 16-byte array with the digest.
 */

#include "HsFFI.h"
#include "md5.h"
#include "Rts.h"
#include "rts/Md5.h"
#include <string.h>

void __hsbase_MD5Init(struct MD5Context *context);
void __hsbase_MD5Update(struct MD5Context *context, byte const *buf, int len);
void __hsbase_MD5Final(byte digest[16], struct MD5Context *context);
void __hsbase_MD5Transform(word32 buf[4], word32 const in[16]);

void
__hsbase_MD5Init(struct MD5Context *ctx)
{
        stg_MD5Init(ctx);
}

/*
 * Update context to reflect the concatenation of another buffer full
 * of bytes.
 */
void
__hsbase_MD5Update(struct MD5Context *ctx, byte const *buf, int len)
{
        stg_MD5Update(ctx, buf, len);
}

/*
 * Final wrapup - pad to 64-byte boundary with the bit pattern 
 * 1 0* (64-bit count of bits processed, MSB-first)
 */
void
__hsbase_MD5Final(byte digest[16], struct MD5Context *ctx)
{
        stg_MD5Final(digest, ctx);
}

/*
 * The core of the MD5 algorithm, this alters an existing MD5 hash to
 * reflect the addition of 16 longwords of new data.  MD5Update blocks
 * the data and converts bytes into longwords for this routine.
 */

void
__hsbase_MD5Transform(word32 buf[4], word32 const in[16])
{
        stg_MD5Transform(buf, in);
}

