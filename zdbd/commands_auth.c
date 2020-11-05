#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/random.h>
#include <inttypes.h>
#include <time.h>
#include "libzdb.h"
#include "zdbd.h"
#include "redis.h"
#include "commands.h"

static int command_auth_challenge(redis_client_t *client) {
    char buffer[8];
    char *string;

    if(getentropy(buffer, sizeof(buffer)) < 0) {
        zdbd_warnp("getentropy");
        redis_hardsend(client, "-Internal random error");
        return 1;
    }

    if(!(string = malloc((sizeof(buffer) * 2) + 1))) {
        zdbd_warnp("challenge: malloc");
        redis_hardsend(client, "-Internal memory error");
        return 1;
    }

    for(unsigned int i = 0; i < sizeof(buffer); i++)
        sprintf(string + (i * 2), "%02x", buffer[i] & 0xff);

    zdbd_debug("[+] auth: challenge generated: %s\n", string);

    // free (possible) previously allocated nonce
    free(client->nonce);

    // set new challenge
    client->nonce = string;

    // send challenge to client
    char response[32];
    sprintf(response, "+%s\r\n", client->nonce);

    redis_reply_stack(client, response, strlen(response));

    return 0;
}

static int command_auth_regular(redis_client_t *client) {
    resp_request_t *request = client->request;

    zdbd_debug("[+] auth: regular authentication requested\n");

    // generic args validation
    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > 128) {
        redis_hardsend(client, "-Password too long");
        return 1;
    }

    char password[192];
    sprintf(password, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);

    if(strcmp(password, zdbd_rootsettings.adminpwd) == 0) {
        zdbd_debug("[+] auth: regular authentication granted\n");

        client->admin = 1;
        redis_hardsend(client, "+OK");
        return 0;
    }

    redis_hardsend(client, "-Access denied");
    return 1;
}

static int command_auth_secure(redis_client_t *client) {
    resp_request_t *request = client->request;

    zdbd_debug("[+] auth: secure authentication requested\n");

    // generic args validation
    if(!command_args_validate(client, 3))
        return 1;

    // only accept <AUTH SECURE password>
    if(strncasecmp(request->argv[1]->buffer, "SECURE", request->argv[1]->length) != 0) {
        redis_hardsend(client, "-Only SECURE extra method supported");
        return 1;
    }

    // check if this is not a CHALLENGE request
    if(strncasecmp(request->argv[2]->buffer, "CHALLENGE", request->argv[2]->length) == 0) {
        zdbd_debug("[+] auth: challenge requested\n");
        return command_auth_challenge(client);
    }

    // only accept client which previously requested nonce challenge
    if(client->nonce == NULL) {
        redis_hardsend(client, "-No CHALLENGE requested, secure authentication not available");
        return 1;
    }

    zdbd_debug("[+] auth: challenge: %s\n", client->nonce);

    // expecting sha1 hexa-string input
    if(request->argv[2]->length != ZDB_SHA1_DIGEST_STR_LENGTH) {
        redis_hardsend(client, "-Invalid hash length");
        return 1;
    }

    char password[64];
    sprintf(password, "%.*s", request->argv[2]->length, (char *) request->argv[2]->buffer);

    char *hashmatch;
    if(asprintf(&hashmatch, "%s:%s", client->nonce, zdbd_rootsettings.adminpwd) < 0) {
        zdbd_warnp("asprintf");
        redis_hardsend(client, "-Internal memory error");
        return 1;
    }

    // compute sha1 and build hex-string
    char buffer[ZDB_SHA1_DIGEST_LENGTH];
    char bufferstr[ZDB_SHA1_DIGEST_STR_LENGTH];

    zdb_sha1(buffer, hashmatch, strlen(hashmatch));
    for(int i = 0; i < ZDB_SHA1_DIGEST_LENGTH; i++)
        sprintf(bufferstr + (i * 2), "%02x", buffer[i] & 0xff);

    zdbd_debug("[+] auth: expected hash: %s\n", bufferstr);
    zdbd_debug("[+] auth: user hash: %s\n", password);

    if(strcmp(password, bufferstr) == 0) {
        zdbd_debug("[+] auth: secure authentication granted\n");

        free(client->nonce);
        free(hashmatch);

        client->nonce = NULL;
        client->admin = 1;

        redis_hardsend(client, "+OK");
        return 0;
    }

    // reset nonce
    free(client->nonce);
    free(hashmatch);
    client->nonce = NULL;

    redis_hardsend(client, "-Access denied");

    return 1;
}

int command_auth(redis_client_t *client) {
    if(!zdbd_rootsettings.adminpwd) {
        redis_hardsend(client, "-Authentication disabled");
        return 0;
    }

    // AUTH <password>
    if(client->request->argc == 2)
        return command_auth_regular(client);

    // AUTH SECURE <password>
    // AUTH SECURE CHALLENGE
    if(client->request->argc == 3)
        return command_auth_secure(client);

    // invalid arguments
    redis_hardsend(client, "-Unexpected arguments");
    return 1;
}

