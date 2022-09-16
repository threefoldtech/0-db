#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <inttypes.h>
#include <time.h>
#include "libzdb.h"
#include "zdbd.h"
#include "redis.h"
#include "commands.h"
#include "commands_get.h"
#include "commands_set.h"
#include "commands_scan.h"
#include "commands_dataset.h"
#include "commands_namespace.h"
#include "commands_system.h"
#include "commands_history.h"
#include "commands_replicate.h"

#define WAIT_MAX_TIMEOUT_MS   30 * 60 * 1000  // 30 min

// ensure number of arguments and their validity
static int real_command_args_validate(redis_client_t *client, int expected, int nullallowed) {
    if(client->request->argc != expected) {
        redis_hardsend(client, "-Unexpected arguments");
        return 0;
    }

    // if we accept null (empty) arguments, we are done
    if(nullallowed)
        return 1;

    // checking for empty arguments
    for(int i = 0; i < expected; i++) {
        if(client->request->argv[i]->length == 0) {
            redis_hardsend(client, "-Invalid argument");
            return 0;
        }
    }

    return 1;
}

// same as validate, but not expecting exact amount of arguments, but
// a minimum value
int command_args_validate_min(redis_client_t *client, int expected) {
    if(client->request->argc < expected) {
        redis_hardsend(client, "-Missing arguments");
        return 0;
    }

    // checking for empty arguments
    for(int i = 0; i < client->request->argc; i++) {
        if(client->request->argv[i]->length == 0) {
            redis_hardsend(client, "-Invalid argument");
            return 0;
        }
    }

    return 1;
}

int command_args_validate_null(redis_client_t *client, int expected) {
    return real_command_args_validate(client, expected, 1);
}

int command_args_validate(redis_client_t *client, int expected) {
    return real_command_args_validate(client, expected, 0);
}

int command_admin_authorized(redis_client_t *client) {
    if(!client->admin) {
        // update failed statistics
        zdbd_rootsettings.stats.adminfailed += 1;

        redis_hardsend(client, "-Permission denied");
        return 0;
    }

    return 1;
}

int command_args_overflow(redis_client_t *client, int argidx, int maxlen) {
    resp_request_t *request = client->request;

    if(request->argv[argidx]->length > maxlen) {
        redis_hardsend(client, "-Argument too long");
        return 0;
    }

    return 1;
}

int command_args_namespace(redis_client_t *client, int argidx) {
    resp_request_t *request = client->request;

    if(request->argv[argidx]->length > NAMESPACE_MAX_LENGTH) {
        redis_hardsend(client, "-Namespace too long");
        return 0;
    }

    return 1;
}

// command parser
// dispatch command to the right handler

static command_t commands_handlers[] = {
    // replication
    {.command = "*",       .handler = command_asterisk}, // special command used to match all in WAIT
    {.command = "WAIT",    .handler = command_wait},     // custom WAIT command to wait on events
    {.command = "MIRROR",  .handler = command_mirror},   // custom MIRROR command to sync full network traffic
    {.command = "MASTER",  .handler = command_master},   // custom MASTER command to flag client as sync source

    // system
    {.command = "PING",    .handler = command_ping},     // default PING command
    {.command = "TIME",    .handler = command_time},     // default TIME command
    {.command = "AUTH",    .handler = command_auth},     // custom AUTH command to authentifcate admin
    {.command = "HOOKS",   .handler = command_hooks},    // custom HOOKS command to list running hooks
    {.command = "INDEX",   .handler = command_index},    // custom INDEX command to query internal index

    // dataset
    {.command = "SET",     .handler = command_set},      // default SET command
    {.command = "SETX",    .handler = command_set},      // alias for SET command
    {.command = "GET",     .handler = command_get},      // default GET command
    {.command = "MGET",    .handler = command_mget},     // default MGET command (multiple get)
    {.command = "DEL",     .handler = command_del},      // default DEL command
    {.command = "EXISTS",  .handler = command_exists},   // default EXISTS command
    {.command = "CHECK",   .handler = command_check},    // custom command to verify data integrity
    {.command = "SCAN",    .handler = command_scan},     // modified SCAN which walk forward dataset
    {.command = "SCANX",   .handler = command_scan},     // alias for SCAN command
    {.command = "RSCAN",   .handler = command_rscan},    // custom command to walk backward dataset
    {.command = "KSCAN",   .handler = command_kscan},    // custom command to iterate over keys matching pattern
    {.command = "HISTORY", .handler = command_history},  // custom command to get previous version of a key
    {.command = "KEYCUR",  .handler = command_keycur},   // custom command to get cursor id from a key

    // query
    {.command = "INFO",    .handler = command_info},     // returns 0-db server name
    {.command = "STOP",    .handler = command_stop},     // custom command for debug purposes

    // namespace
    {.command = "DBSIZE",  .handler = command_dbsize},   // default DBSIZE command
    {.command = "NSNEW",   .handler = command_nsnew},    // custom command to create a namespace
    {.command = "NSDEL",   .handler = command_nsdel},    // custom command to remove a namespace
    {.command = "NSLIST",  .handler = command_nslist},   // custom command to list namespaces
    {.command = "NSSET",   .handler = command_nsset},    // custom command to edit namespace settings
    {.command = "NSINFO",  .handler = command_nsinfo},   // custom command to get namespace information
    {.command = "SELECT",  .handler = command_select},   // default SELECT (with pwd) namespace switch
    {.command = "RELOAD",  .handler = command_reload},   // custom command to reload a namespace
    {.command = "FLUSH",   .handler = command_flush},    // custom command to reset a namespace
};

int redis_dispatcher(redis_client_t *client) {
    resp_request_t *request = client->request;
    resp_object_t *key = request->argv[0];

    // client doesn't have a running namespace
    // this will happens when a namespace is removed
    // and the client is still attached to this namespace
    //
    // in that special case, we notify this client it's namespace
    // is not available anymore and we disconnect it
    if(client->ns == NULL) {
        zdbd_debug("[-] command: request fd: %d, no namespace, disconnecting.\n", client->fd);
        redis_hardsend(client, "-Your active namespace is not available anymore (probably removed).");
        return RESP_STATUS_DISCARD;
    }

    zdbd_debug("[+] command: request fd: %d, namespace: %s\n", client->fd, client->ns->name);
    client->commands += 1;

    if(key->type != STRING) {
        zdbd_debug("[-] command: not a string command, ignoring\n");
        return 0;
    }

    zdbd_debug("[+] command: '%.*s' [+%d args]\n", key->length, (char *) key->buffer, request->argc - 1);

    for(unsigned int i = 0; i < sizeof(commands_handlers) / sizeof(command_t); i++) {
        if(strncasecmp(key->buffer, commands_handlers[i].command, key->length) == 0) {
            // save last command executed
            client->executed = &commands_handlers[i];

            // update statistics
            zdbd_rootsettings.stats.cmdsvalid += 1;

            // execute handler
            return commands_handlers[i].handler(client);
        }
    }

    // unknown command
    zdbd_verbose("[-] command: unsupported redis command\n");
    zdbd_rootsettings.stats.cmdsfailed += 1;

    // reset executed flag, this was a non-existing command
    client->executed = NULL;
    redis_hardsend(client, "-Command not supported");

    return 1;
}

// set the client to wait on a special handler to be triggered
int command_wait(redis_client_t *client) {
    resp_request_t *request = client->request;
    command_t *handler = NULL;

    if(client->request->argc != 2 && client->request->argc != 3) {
        redis_hardsend(client, "-Invalid arguments");
        return 1;
    }

    // extract argument
    resp_object_t *key = request->argv[1];

    // checking if the requested command is supported
    for(unsigned int i = 0; i < sizeof(commands_handlers) / sizeof(command_t); i++) {
        if(strncasecmp(key->buffer, commands_handlers[i].command, key->length) == 0) {
            handler = &commands_handlers[i];
            break;
        }
    }

    if(handler == NULL) {
        redis_hardsend(client, "-Unknown command to watch");
        return 0;
    }

    size_t timeout = 5000;

    if(client->request->argc == 3) {
        char buffer[20];

        if(client->request->argv[2]->length > 16) {
            redis_hardsend(client, "-Invalid timeout");
            return 1;
        }

        memset(buffer, 0, sizeof(buffer));
        strncpy(buffer, client->request->argv[2]->buffer, client->request->argv[2]->length);

        timeout = strtoull(buffer, NULL, 10);
        if(timeout < 100 || timeout > WAIT_MAX_TIMEOUT_MS) {
            redis_hardsend(client, "-Invalid timeout");
            return 1;
        }
    }

    // set watching flag
    redis_client_set_watcher(client, handler, timeout);

    return 1;
}

// dummy handler which does nothing
// this is only used to match 'WAIT *' command dispatching
int command_asterisk(redis_client_t *client) {
    redis_hardsend(client, "-This is not a valid command");
    return 0;
}

// generic error for locked namespace
int command_error_locked(redis_client_t *client) {
    zdbd_debug("[-] command: request interrupted, namespace locked\n");
    redis_hardsend(client, "-Namespace is temporarily locked (read-only)");
    return 1;
}

// generic error for frozen namespace
int command_error_frozen(redis_client_t *client) {
    zdbd_debug("[-] command: request interrupted, namespace frozen\n");
    redis_hardsend(client, "-Namespace is temporarily frozen (read-write disabled)");
    return 1;
}
