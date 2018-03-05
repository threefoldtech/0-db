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
#include "commands_get.h"

//
// SCAN
//
static int command_scan_userkey(resp_request_t *request) {
    redis_hardsend(request->client->fd, "-Implementing (userkey)");
    return 0;
}

static int command_scan_sequential(resp_request_t *request) {

    redis_hardsend(request->client->fd, "-Implementing (sequential)");
    return 0;
}

static int command_scan_directkey(resp_request_t *request) {
    redis_hardsend(request->client->fd, "-Implementing (directkey)");
    return 0;
}

int (*command_scan_handlers[])(resp_request_t *request) = {
    command_scan_userkey,
    command_scan_sequential,
    command_scan_directkey,
};

int command_scan(resp_request_t *request) {
    return command_scan_handlers[rootsettings.mode](request);
}


//
// RSCAN
//
static int command_rscan_userkey(resp_request_t *request) {
    redis_hardsend(request->client->fd, "-Implementing (userkey)");
    return 0;
}

static int command_rscan_sequential(resp_request_t *request) {

    redis_hardsend(request->client->fd, "-Implementing (sequential)");
    return 0;
}

static int command_rscan_directkey(resp_request_t *request) {
    redis_hardsend(request->client->fd, "-Implementing (directkey)");
    return 0;
}

int (*command_rscan_handlers[])(resp_request_t *request) = {
    command_rscan_userkey,
    command_rscan_sequential,
    command_rscan_directkey,
};

int command_rscan(resp_request_t *request) {
    return command_rscan_handlers[rootsettings.mode](request);
}


