#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <inttypes.h>
#include "zerodb.h"
#include "index.h"
#include "data.h"
#include "namespace.h"
#include "redis.h"
#include "commands.h"

int command_ping(redis_client_t *client) {
    redis_hardsend(client->fd, "+PONG");
    return 0;
}

int command_time(redis_client_t *client) {
    struct timeval now;
    char response[256];
    char sec[64], usec[64];

    gettimeofday(&now, NULL);

    // default redis protocol returns values as string
    // not as integer
    sprintf(sec, "%lu", now.tv_sec);
    sprintf(usec, "%ld", (long) now.tv_usec);

    // *2             \r\n  -- array of two values
    // $[sec-length]  \r\n  -- first header, string value
    // [seconds]      \r\n  -- first value payload
    // $[usec-length] \r\n  -- second header, string value
    // [usec]         \r\n  -- second value payload
    sprintf(response, "*2\r\n$%lu\r\n%s\r\n$%lu\r\n%s\r\n", strlen(sec), sec, strlen(usec), usec);

    send(client->fd, response, strlen(response), 0);

    return 0;
}

int command_auth(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(!command_args_validate(client, 2))
        return 1;

    if(!rootsettings.adminpwd) {
        redis_hardsend(client->fd, "-Authentification disabled");
        return 0;
    }

    if(request->argv[1]->length > 128) {
        redis_hardsend(client->fd, "-Password too long");
        return 1;
    }

    char password[192];
    sprintf(password, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);

    if(strcmp(password, rootsettings.adminpwd) == 0) {
        client->admin = 1;
        redis_hardsend(client->fd, "+OK");
        return 0;
    }

    redis_hardsend(client->fd, "-Access denied");
    return 1;
}

// STOP will be only compiled in debug mode
// this will force to exit listen loop in order to call
// all destructors, this is useful to ensure every memory allocation
// are well tracked and well cleaned
//
// in production, a user should not be able to stop the daemon
int command_stop(redis_client_t *client) {
    #ifndef RELEASE
        redis_hardsend(client->fd, "+Stopping");
        return RESP_STATUS_SHUTDOWN;
    #else
        redis_hardsend(client->fd, "-Unauthorized");
        return 0;
    #endif
}

int command_info(redis_client_t *client) {
    redis_hardsend(client->fd, "+0-db server (" REVISION ")\n");
    return 0;
}

