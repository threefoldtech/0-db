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


