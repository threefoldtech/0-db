#ifndef __ZDB_SHA1_H
    #define __ZDB_SHA1_H

    #define ZDB_SHA1_DIGEST_STR_LENGTH  40
    #define ZDB_SHA1_DIGEST_LENGTH      20

    void zdb_sha1(char *hash, const char *str, unsigned int len);
#endif
