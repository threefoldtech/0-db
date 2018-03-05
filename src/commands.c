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
#include "commands_set.h"
#include "commands_scan.h"
#include "commands_dataset.h"
#include "commands_namespace.h"
#include "commands_system.h"

// ensure number of argument and their validity
int command_args_validate(resp_request_t *request, int expected) {
    if(request->argc != expected) {
        redis_hardsend(request->client->fd, "-Unexpected arguments");
        return 0;
    }

    for(int i = 0; i < expected; i++) {
        if(request->argv[i]->length == 0) {
            redis_hardsend(request->client->fd, "-Invalid argument");
            return 0;
        }
    }

    return 1;
}

int command_admin_authorized(resp_request_t *request) {
    if(!request->client->admin) {
        redis_hardsend(request->client->fd, "-Permission denied");
        return 0;
    }

    return 1;
}

// command parser
// dispatch command to the right handler

static command_t commands_handlers[] = {
    // system
    {.command = "PING", .handler = command_ping}, // default PING command
    {.command = "TIME", .handler = command_time}, // default TIME command
    {.command = "AUTH", .handler = command_auth}, // custom AUTH command to authentifcate admin

    // dataset
    {.command = "SET",    .handler = command_set},    // default SET command
    {.command = "SETX",   .handler = command_set},    // alias for SET command
    {.command = "GET",    .handler = command_get},    // default GET command
    {.command = "DEL",    .handler = command_del},    // default DEL command
    {.command = "EXISTS", .handler = command_exists}, // default EXISTS command
    {.command = "CHECK",  .handler = command_check},  // custom command to verify data integrity
    {.command = "SCAN",   .handler = command_scan},   // modified SCAN which walk forward dataset
    {.command = "RSCAN",  .handler = command_rscan},  // custom command to walk backward dataset

    // query
    {.command = "INFO", .handler = command_info}, // returns 0-db server name
    {.command = "STOP", .handler = command_stop}, // custom command for debug purpose

    // namespace
    {.command = "DBSIZE", .handler = command_dbsize},  // default DBSIZE command
    {.command = "NSNEW",  .handler = command_nsnew},   // custom command to create a namespace
    {.command = "NSLIST", .handler = command_nslist},  // custom command to list namespaces
    {.command = "NSSET",  .handler = command_nsset},   // custom command to edit namespace settings
    {.command = "NSINFO", .handler = command_nsinfo},  // custom command to get namespace information
    {.command = "SELECT", .handler = command_select},  // default SELECT (with pwd) namespace switch
};

int redis_dispatcher(resp_request_t *request) {
    resp_object_t *key = request->argv[0];

    debug("[+] command: request fd: %d, namespace: %s\n", request->client->fd, request->client->ns->name);
    request->client->commands += 1;

    if(key->type != STRING) {
        debug("[-] command: not a string command, ignoring\n");
        return 0;
    }

    debug("[+] command: '%.*s' [+%d args]\n", key->length, (char *) key->buffer, request->argc - 1);

    for(unsigned int i = 0; i < sizeof(commands_handlers) / sizeof(command_t); i++) {
        if(strncmp(key->buffer, commands_handlers[i].command, key->length) == 0)
            return commands_handlers[i].handler(request);
    }

    // unknown
    printf("[-] command: unsupported redis command\n");
    redis_hardsend(request->client->fd, "-Command not supported");

    return 1;
}

