#ifndef __ZDBD_SHA1_H
    #define __ZDBD_SHA1_H

    #define SHA1_DIGEST_STR_LENGTH  40
    #define SHA1_DIGEST_LENGTH      20

    void sha1(char *hash, const char *str, unsigned int len);
#endif
