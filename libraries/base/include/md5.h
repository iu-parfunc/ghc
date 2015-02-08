/* MD5 message digest */
#ifndef _MD5_H
#define _MD5_H

#include "HsFFI.h"
#include "rts/Md5.h"

typedef HsWord32 word32;
typedef HsWord8  byte;

void __hsbase_MD5Init(struct StgMD5Context *context);
void __hsbase_MD5Update(struct StgMD5Context *context, byte const *buf, int len);
void __hsbase_MD5Final(byte digest[16], struct StgMD5Context *context);
void __hsbase_MD5Transform(word32 buf[4], word32 const in[16]);

#endif /* _MD5_H */



