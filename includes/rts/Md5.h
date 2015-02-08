/* MD5 message digest - for use by base.GHC.Fingerprint only */
#ifndef RTS_MD5_H
#define RTS_MD5_H

#include "stg/Types.h"

typedef StgWord32 word32;
typedef StgWord8  byte;

struct StgMD5Context {
	word32 buf[4];
	word32 bytes[2];
	word32 in[16];
};

void stg_MD5Init(struct StgMD5Context *context);
void stg_MD5Update(struct StgMD5Context *context, const byte const *buf, unsigned int len);
void stg_MD5Final(byte digest[16], struct StgMD5Context *context);
void stg_MD5Transform(word32 buf[4], word32 const in[16]);

#endif /* RTS_MD5_H */



