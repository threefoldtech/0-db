#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/random.h>
#include "libzdb.h"
#include "libzdb_private.h"

// zdb_challenge generate a random string
// which can be used for cryptographic random
// this can be used to salt stuff and generate nonce
char *zdb_challenge() {
    char buffer[8];
    char *string;

    if(getentropy(buffer, sizeof(buffer)) < 0) {
        zdb_warnp("getentropy");
        return NULL;
    }

    if(!(string = malloc((sizeof(buffer) * 2) + 1))) {
        zdb_warnp("challenge: malloc");
        return NULL;
    }

    for(unsigned int i = 0; i < sizeof(buffer); i++)
        sprintf(string + (i * 2), "%02x", buffer[i] & 0xff);

    zdb_debug("[+] security: challenge generated: %s\n", string);

    return string;
}

// generate an allocated string, which contains hexahash of
// sha1 concatenated with password with colon
//   sha1(salt:password)
// string needs to be free'd after use
char *zdb_hash_password(char *salt, char *password) {
    char *hashmatch;

    if(asprintf(&hashmatch, "%s:%s", salt, password) < 0) {
        zdb_warnp("asprintf");
        return NULL;
    }

    char buffer[ZDB_SHA1_DIGEST_LENGTH + 1];
    char bufferstr[ZDB_SHA1_DIGEST_STR_LENGTH + 1];

    memset(buffer, 0, sizeof(buffer));
    memset(bufferstr, 0, sizeof(bufferstr));

    // compute sha1 and build hex-string
    zdb_sha1(buffer, hashmatch, strlen(hashmatch));

    for(int i = 0; i < ZDB_SHA1_DIGEST_LENGTH; i++)
        sprintf(bufferstr + (i * 2), "%02x", buffer[i] & 0xff);

    free(hashmatch);

    return strdup(bufferstr);
}
