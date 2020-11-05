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
#include "auth.h"

static int command_auth_challenge(redis_client_t *client) {
    char *string;

    if(!(string = zdb_challenge())) {
        redis_hardsend(client, "-Internal generator error");
        return 1;
    }

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

    resp_object_t *user = request->argv[1];

    if(zdbd_password_check(user->buffer, user->length, zdbd_rootsettings.adminpwd)) {
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

    char *expected;

    if(!(expected = zdb_hash_password(client->nonce, zdbd_rootsettings.adminpwd))) {
        redis_hardsend(client, "-Internal generator error");
        return 1;
    }

    zdbd_debug("[+] auth: expected hash: %s\n", expected);

    resp_object_t *user = request->argv[2];
    int granted = 0;

    if(zdbd_password_check(user->buffer, user->length, expected)) {
        zdbd_debug("[+] auth: secure authentication granted\n");

        // flag user as authenticated
        client->admin = 1;
        granted = 1;

        redis_hardsend(client, "+OK");

    } else {
        // wrong password
        redis_hardsend(client, "-Access denied");
    }

    // free and reset nonce
    free(expected);
    free(client->nonce);
    client->nonce = NULL;

    return granted;
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

