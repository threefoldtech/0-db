#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libzdb.h"
#include "zdbd.h"

// check if user input (buffer with buffer length)
// does match with expected password string, unified way
int zdbd_password_check(char *input, int length, char *expected) {
    char password[192];
    sprintf(password, "%.*s", length, input);

    if(strcmp(password, expected) == 0) {
        zdbd_debug("[+] password: access granted\n");
        return 1;
    }

    zdbd_debug("[-] password: wrong password\n");
    return 0;
}

// generate an allocated string, which contains hexahash of
// sha1 concatenated with password with colon
//   sha1(salt:password)
// string needs to be free'd after use
char *zdb_hash_password(char *salt, char *password) {
    char *hashmatch;

    if(asprintf(&hashmatch, "%s:%s", salt, password) < 0) {
        zdbd_warnp("asprintf");
        return NULL;
    }

    char buffer[ZDB_SHA1_DIGEST_LENGTH];
    char bufferstr[ZDB_SHA1_DIGEST_STR_LENGTH];

    // compute sha1 and build hex-string
    zdb_sha1(buffer, hashmatch, strlen(hashmatch));

    for(int i = 0; i < ZDB_SHA1_DIGEST_LENGTH; i++)
        sprintf(bufferstr + (i * 2), "%02x", buffer[i] & 0xff);

    free(hashmatch);

    return strdup(bufferstr);
}
