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

// tools

// ensure number of argument and their validity
static int command_args_validate(resp_request_t *request, int expected) {
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

//
//
// specific modes implementation
// some commands does specific stuff in differents modes
// each of them are implemented in distinct functions
//
//

//
// different SET implementation
// depending on the running mode
//
static size_t redis_set_handler_userkey(resp_request_t *request) {
    // create some easier accessor
    unsigned char *id = request->argv[1]->buffer;
    uint8_t idlength = request->argv[1]->length;

    unsigned char *value = request->argv[2]->buffer;
    uint32_t valuelength = request->argv[2]->length;

    // shortcut to index and data
    index_root_t *index = request->client->ns->index;
    data_root_t *data = request->client->ns->data;

    debug("[+] command: set: %u bytes key, %u bytes data\n", idlength, valuelength);
    // printf("[+] set key: %.*s\n", idlength, id);
    // printf("[+] set value: %.*s\n", request->argv[2]->length, (char *) request->argv[2]->buffer);

    // insert the data on the datafile
    // this will returns us the offset where the header is
    size_t offset = data_insert(data, value, valuelength, id, idlength);

    // checking for writing error
    // if we couldn't write the data, we won't add entry on the index
    // and report to the client an error
    if(offset == 0) {
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    debug("[+] command: set: userkey: ");
    debughex(id, idlength);
    debug("\n");

    debug("[+] command: set: offset: %lu\n", offset);

    // inserting this offset with the id on the index
    if(!index_entry_insert(index, id, idlength, offset, request->argv[2]->length)) {
        // cannot insert index (disk issue)
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    // building response
    // here, from original redis protocol, we don't reply with a basic
    // OK or Error when inserting a key, we reply with the key itself
    //
    // this is how the sequential-id can returns the id generated
    redis_bulk_t response = redis_bulk(id, idlength);
    if(!response.buffer) {
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    send(request->client->fd, response.buffer, response.length, 0);
    free(response.buffer);

    return offset;
}

static size_t redis_set_handler_sequential(resp_request_t *request) {
    // shortcut to index and data
    index_root_t *index = request->client->ns->index;
    data_root_t *data = request->client->ns->data;

    // create some easier accessor
    uint32_t id = index_next_id(index);
    uint8_t idlength = sizeof(uint32_t);

    unsigned char *value = request->argv[2]->buffer;
    uint32_t valuelength = request->argv[2]->length;

    debug("[+] command: set: %u bytes key, %u bytes data\n", idlength, valuelength);

    // insert the data on the datafile
    // this will returns us the offset where the header is
    // size_t offset = data_insert(value, valuelength, id, idlength);
    size_t offset = data_insert(data, value, valuelength, &id, idlength);

    // checking for writing error
    // if we couldn't write the data, we won't add entry on the index
    // and report to the client an error
    if(offset == 0) {
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    debug("[+] command: set: sequential-key: ");
    debughex(&id, idlength);
    debug("\n");

    debug("[+] command: set: offset: %lu\n", offset);

    // inserting this offset with the id on the index
    // if(!index_entry_insert(id, idlength, offset, request->argv[2]->length)) {
    if(!index_entry_insert(index, &id, idlength, offset, request->argv[2]->length)) {
        // cannot insert index (disk issue)
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    // building response
    // here, from original redis protocol, we don't reply with a basic
    // OK or Error when inserting a key, we reply with the key itself
    //
    // this is how the sequential-id can returns the id generated
    // redis_bulk_t response = redis_bulk(id, idlength);
    redis_bulk_t response = redis_bulk(&id, idlength);
    if(!response.buffer) {
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    send(request->client->fd, response.buffer, response.length, 0);
    free(response.buffer);

    return offset;
}

static size_t redis_set_handler_directkey(resp_request_t *request) {
    // shortcut to data
    data_root_t *data = request->client->ns->data;

    // create some easier accessor
    index_dkey_t id = {
        .dataid = data_dataid(data), // current data fileid
        .offset = 0                  // will be filled later by data_insert
    };
    uint8_t idlength = sizeof(index_dkey_t);

    unsigned char *value = request->argv[2]->buffer;
    uint32_t valuelength = request->argv[2]->length;


    debug("[+] command: set: %u bytes key, %u bytes data\n", idlength, valuelength);

    // insert the data on the datafile
    // this will returns us the offset where the header is
    // size_t offset = data_insert(value, valuelength, id, idlength);
    size_t offset = data_insert(data, value, valuelength, &id, idlength);
    id.offset = offset;

    // checking for writing error
    // if we couldn't write the data, we won't add entry on the index
    // and report to the client an error
    if(offset == 0) {
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    debug("[+] command: set: direct-key: ");
    debughex(&id, idlength);
    debug("\n");

    debug("[+] command: set: offset: %lu\n", offset);

    // usually, here is where we add the key in the index
    // we don't use the index in this case since the position
    // is the key itself

    // building response
    // here, from original redis protocol, we don't reply with a basic
    // OK or Error when inserting a key, we reply with the key itself
    //
    // this is how the sequential-id can returns the id generated
    // redis_bulk_t response = redis_bulk(id, idlength);
    redis_bulk_t response = redis_bulk(&id, idlength);
    if(!response.buffer) {
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    send(request->client->fd, response.buffer, response.length, 0);
    free(response.buffer);

    return offset;
}

static size_t (*redis_set_handlers[])(resp_request_t *request) = {
    redis_set_handler_userkey,    // key-value mode
    redis_set_handler_sequential, // incremental mode
    redis_set_handler_directkey,  // direct-key mode
};


//
// different GET implementation
// depending on the running mode
//
static index_entry_t *redis_get_handler_memkey(resp_request_t *request) {
    index_root_t *index = request->client->ns->index;
    return index_entry_get(index, request->argv[1]->buffer, request->argv[1]->length);
}

static index_entry_t *redis_get_handler_direct(resp_request_t *request) {
    // invalid requested key
    if(request->argv[1]->length != sizeof(index_dkey_t))
        return NULL;

    // converting binary key to internal struct
    index_dkey_t directkey;

    memcpy(&directkey, request->argv[1]->buffer, sizeof(index_dkey_t));
    memcpy(index_reusable_entry->id, request->argv[1]->buffer, sizeof(index_dkey_t));

    index_reusable_entry->idlength = sizeof(index_dkey_t);
    index_reusable_entry->offset = directkey.offset;
    index_reusable_entry->dataid = directkey.dataid;

    // when using a zero-length payload
    // the length will be used from the data header
    // and not from tne index
    index_reusable_entry->length = 0;

    return index_reusable_entry;
}

static index_entry_t * (*redis_get_handlers[])(resp_request_t *request) = {
    redis_get_handler_memkey, // key-value mode
    redis_get_handler_memkey, // incremental mode
    redis_get_handler_direct, // direct-key mode
};

//
//
// commands implementation
// each redis command are implemented in a specific function
//
//
static int command_ping(resp_request_t *request) {
    redis_hardsend(request->client->fd, "+PONG");
    return 0;
}

static int command_set(resp_request_t *request) {
    if(!command_args_validate(request, 3))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        redis_hardsend(request->client->fd, "-Key too large");
        return 1;
    }

    size_t offset = redis_set_handlers[rootsettings.mode](request);
    if(offset == 0)
        return 0;

    // checking if we need to jump to the next files
    // we do this check here and not from data (event if this is like a
    // datafile event) to keep data and index code completly distinct
    if(offset + request->argv[2]->length > 256 * 1024 * 1024) { // 256 MB
        size_t newid = index_jump_next(request->client->ns->index);
        data_jump_next(request->client->ns->data, newid);
    }

    return 0;
}

static int command_get(resp_request_t *request) {
    if(!command_args_validate(request, 2))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        printf("[-] invalid key size\n");
        redis_hardsend(request->client->fd, "-Invalid key");
        return 1;
    }

    debug("[+] command: get: lookup key: ");
    debughex(request->argv[1]->buffer, request->argv[1]->length);
    debug("\n");

    index_entry_t *entry = redis_get_handlers[rootsettings.mode](request);

    // key not found at all
    if(!entry) {
        verbose("[-] command: get: key not found\n");
        redis_hardsend(request->client->fd, "$-1");
        return 1;
    }

    // key found but deleted
    if(entry->flags & INDEX_ENTRY_DELETED) {
        verbose("[-] command: get: key deleted\n");
        redis_hardsend(request->client->fd, "$-1");
        return 1;
    }

    // key found and valid, let's checking the contents
    debug("[+] command: get: entry found, flags: %x, data length: %" PRIu64 "\n", entry->flags, entry->length);
    debug("[+] command: get: data file: %d, data offset: %" PRIu64 "\n", entry->dataid, entry->offset);

    data_root_t *data = request->client->ns->data;
    data_payload_t payload = data_get(data, entry->offset, entry->length, entry->dataid, entry->idlength);

    if(!payload.buffer) {
        printf("[-] command: get: cannot read payload\n");
        redis_hardsend(request->client->fd, "-Internal Error");
        free(payload.buffer);
        return 0;
    }

    redis_bulk_t response = redis_bulk(payload.buffer, payload.length);
    if(!response.buffer) {
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    send(request->client->fd, response.buffer, response.length, 0);

    free(response.buffer);
    free(payload.buffer);

    return 0;
}

static int command_exists(resp_request_t *request) {
    if(!command_args_validate(request, 2))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        printf("[-] invalid key size\n");
        redis_hardsend(request->client->fd, "-Invalid key");
        return 1;
    }

    debug("[+] command: exists: lookup key: ");
    debughex(request->argv[1]->buffer, request->argv[1]->length);
    debug("\n");

    index_entry_t *entry = redis_get_handlers[rootsettings.mode](request);

    // FIXME: will gives fake positive on direct-mode

    debug("[+] command: exists: entry found: %s\n", (entry ? "yes" : "no"));
    int found = (entry == NULL) ? 0 : 1;

    char response[32];
    sprintf(response, ":%d\r\n", found);

    send(request->client->fd, response, strlen(response), 0);

    return 0;
}

static int command_del(resp_request_t *request) {
    if(!command_args_validate(request, 2))
        return 1;

    // disallow delete key on direct mode, we don't have index
    // we can't flag key as deleted and data files are always append
    if(rootsettings.mode == DIRECTKEY) {
        redis_hardsend(request->client->fd, "-Unsupported on this mode");
        return 0;
    }

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        printf("[-] command: del: invalid key size\n");
        redis_hardsend(request->client->fd, "-Invalid key");
        return 1;
    }

    index_root_t *index = request->client->ns->index;

    if(!index_entry_delete(index, request->argv[1]->buffer, request->argv[1]->length)) {
        redis_hardsend(request->client->fd, "-Cannot delete key");
        return 0;
    }

    redis_hardsend(request->client->fd, "+OK");

    return 0;
}

static int command_info(resp_request_t *request) {
    redis_hardsend(request->client->fd, "+0-db server (" REVISION ")\n");
    return 0;
}

// create a new namespace
//   NSNEW [namespace]
static int command_nsnew(resp_request_t *request) {
    char target[COMMAND_MAXLEN];

    if(!command_args_validate(request, 2))
        return 1;

    if(request->argv[1]->length > 128) {
        redis_hardsend(request->client->fd, "-Namespace too long");
        return 1;
    }

    // get string formatted namespace
    sprintf(target, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);

    // deny already existing namespace
    if(namespace_get(target)) {
        debug("[-] command: mkns: namespace already exists\n");
        redis_hardsend(request->client->fd, "-This namespace is not available");
        return 1;
    }

    // creating the new namespace
    // note: password needs to be set via NSSET after creation
    if(!namespace_create(target)) {
        redis_hardsend(request->client->fd, "-Could not create namespace");
        return 1;
    }

    redis_hardsend(request->client->fd, "+OK");

    return 0;
}

// change user active namespace
//   SELECT [namespace]
static int command_select(resp_request_t *request) {
    char target[COMMAND_MAXLEN];
    namespace_t *namespace = NULL;

    if(request->argc < 2) {
        redis_hardsend(request->client->fd, "-Invalid argument");
        return 1;
    }

    if(request->argv[1]->length > 128) {
        redis_hardsend(request->client->fd, "-Namespace too long");
        return 1;
    }

    // get name as usable string
    sprintf(target, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);


    // checking for existing namespace
    if(!(namespace = namespace_get(target))) {
        debug("[-] command: select: namespace not found\n");
        redis_hardsend(request->client->fd, "-Namespace not found");
        return 1;
    }

    // checking if password is set
    if(namespace->password) {
        if(request->argc != 3) {
            redis_hardsend(request->client->fd, "-Namespace protected");
            return 1;
        }

        if(strncmp(request->argv[2]->buffer, namespace->password, request->argv[2]->length) != 0) {
            redis_hardsend(request->client->fd, "-Access denied");
            return 1;
        }

        // access granted
    }

    // switching client's active namespace
    debug("[+] command: select: moving user to namespace '%s'\n", namespace->name);
    request->client->ns = namespace;

    // return confirmation
    redis_hardsend(request->client->fd, "+OK");

    return 0;
}

// list available namespaces (all of them)
//   NSLIST (no arguments)
static int command_nslist(resp_request_t *request) {
    char line[512];
    ns_root_t *nsroot = namespace_get_list();

    // streaming list to the client
    sprintf(line, "*%lu\r\n", nsroot->length);
    send(request->client->fd, line, strlen(line), 0);

    debug("[+] command: nslist: sending %lu items\n", nsroot->length);

    // sending each namespace line by line
    for(size_t i = 0; i < nsroot->length; i++) {
        namespace_t *ns = nsroot->namespaces[i];

        sprintf(line, "$%ld\r\n%s\r\n", strlen(ns->name), ns->name);
        send(request->client->fd, line, strlen(line), 0);
    }

    return 0;
}

// get information about a namespace
//   NSINFO [namespace]
static int command_nsinfo(resp_request_t *request) {
    char info[1024];
    char target[COMMAND_MAXLEN];
    namespace_t *namespace;

    if(!command_args_validate(request, 2))
        return 1;

    if(request->argv[1]->length > 128) {
        redis_hardsend(request->client->fd, "-Namespace too long");
        return 1;
    }

    // get name as usable string
    sprintf(target, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);

    // checking for existing namespace
    if(!(namespace = namespace_get(target))) {
        debug("[-] command: nsinfo: namespace not found\n");
        redis_hardsend(request->client->fd, "-Namespace not found");
        return 1;
    }

    sprintf(info, "# namespace\n");
    sprintf(info + strlen(info), "name: %s\n", namespace->name);
    sprintf(info + strlen(info), "entries: %lu\n", namespace->index->entries);
    sprintf(info + strlen(info), "public: %s\n", namespace->public ? "yes" : "no");
    sprintf(info + strlen(info), "data_size_bytes: %lu\n", namespace->index->datasize);
    sprintf(info + strlen(info), "data_size_mb: %.2f\n", namespace->index->datasize / (1024 * 1024.0));
    sprintf(info + strlen(info), "data_limits_bytes: %lu\n", namespace->maxsize);
    sprintf(info + strlen(info), "index_size_bytes: %lu\n", namespace->index->indexsize);
    sprintf(info + strlen(info), "index_size_kb: %.2f\n", namespace->index->indexsize / 1024.0);

    redis_bulk_t response = redis_bulk(info, strlen(info));
    if(!response.buffer) {
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    send(request->client->fd, response.buffer, response.length, 0);

    free(response.buffer);

    return 0;
}

// change namespace settings
//   NSSET [namespace] password *        -> clear password
//   NSSET [namespace] password [foobar] -> set password to 'foobar'
//   NSSET [namespace] maxsize [123456]  -> set maximum datasize to '123456'
//                                          if this is more than actual size, there
//                                          is no shrink, it stay as it
//   NSSET [namespace] public [1 or 0]   -> enable or disable public access
static int command_nsset(resp_request_t *request) {
    namespace_t *namespace = NULL;
    char target[COMMAND_MAXLEN];
    char command[COMMAND_MAXLEN];
    char value[COMMAND_MAXLEN];

    if(!command_args_validate(request, 4))
        return 1;

    if(request->argv[1]->length > 128) {
        redis_hardsend(request->client->fd, "-Namespace too long");
        return 1;
    }

    if(request->argv[2]->length > COMMAND_MAXLEN || request->argv[3]->length > COMMAND_MAXLEN) {
        redis_hardsend(request->client->fd, "-Argument too long");
        return 1;
    }

    sprintf(target, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);
    sprintf(command, "%.*s", request->argv[2]->length, (char *) request->argv[2]->buffer);
    sprintf(value, "%.*s", request->argv[3]->length, (char *) request->argv[3]->buffer);

    // limit size of the value
    if(request->argv[3]->length > 63) {
        redis_hardsend(request->client->fd, "-Invalid value");
        return 1;
    }

    // default namespace cannot be changed
    if(strcmp(target, NAMESPACE_DEFAULT) == 0) {
        redis_hardsend(request->client->fd, "-Cannot update default namespace");
        return 1;
    }

    // checking for existing namespace
    if(!(namespace = namespace_get(target))) {
        debug("[-] command: nsset: namespace not found\n");
        redis_hardsend(request->client->fd, "-Namespace not found");
        return 1;
    }

    //
    // testing properties
    //
    if(strcmp(command, "maxsize") == 0) {
        namespace->maxsize = atoll(value);
        debug("[+] command: nsset: new size limit: %lu\n", namespace->maxsize);

    } else if(strcmp(command, "password") == 0) {
        // clearing password using "*" password
        if(strcmp(value, "*") == 0) {
            free(namespace->password);
            namespace->password = NULL;

            debug("[+] command: nsset: password cleared\n");

            // updating password
        } else {
            namespace->password = strdup(value);
            debug("[+] command: nsset: password set and updated\n");
        }


    } else if(strcmp(command, "public") == 0) {
        namespace->public = (value[0] == '1') ? 1 : 0;
        debug("[+] command: nsset: changing public view to: %d\n", namespace->public);

    } else {
        debug("[-] command: nsset: unknown property '%s'\n", command);
        redis_hardsend(request->client->fd, "-Invalid property");
        return 1;
    }

    // update persistant setting
    namespace_commit(namespace);

    // confirmation
    redis_hardsend(request->client->fd, "+OK");

    return 0;
}

static int command_dbsize(resp_request_t *request) {
    char response[64];

    sprintf(response, ":%lu\r\n", request->client->ns->index->entries);
    send(request->client->fd, response, strlen(response), 0);

    return 0;
}

static int command_time(resp_request_t *request) {
    struct timeval now;
    char response[256];
    char sec[64], usec[64];

    gettimeofday(&now, NULL);

    // default redis protocol returns values as string
    // not as integer
    sprintf(sec, "%lu", now.tv_sec);
    sprintf(usec, "%ld", now.tv_usec);

    // *2             \r\n  -- array of two values
    // $[sec-length]  \r\n  -- first header, string value
    // [seconds]      \r\n  -- first value payload
    // $[usec-length] \r\n  -- second header, string value
    // [usec]         \r\n  -- second value payload
    sprintf(response, "*2\r\n$%lu\r\n%s\r\n$%lu\r\n%s\r\n", strlen(sec), sec, strlen(usec), usec);

    send(request->client->fd, response, strlen(response), 0);

    return 0;
}

// STOP will be only compiled in debug mode
// this will force to exit listen loop in order to call
// all destructors, this is useful to ensure every memory allocation
// are well tracked and well cleaned
//
// in production, a user should not be able to stop the daemon
static int command_stop(resp_request_t *request) {
    #ifndef RELEASE
        redis_hardsend(request->client->fd, "+Stopping");
        return 2;
    #else
        redis_hardsend(request->client->fd, "-Unauthorized");
        return 0;
    #endif
}

//
//
// command parser
// dispatch command to the right handler
//
//

static command_t commands_handlers[] = {
    // system
    {.command = "PING", .handler = command_ping}, // default PING command
    {.command = "TIME", .handler = command_time}, // default TIME command

    // dataset
    {.command = "SET",    .handler = command_set},    // default SET command
    {.command = "SETX",   .handler = command_set},    // alias for SET command
    {.command = "GET",    .handler = command_get},    // default GET command
    {.command = "DEL",    .handler = command_del},    // default DEL command
    {.command = "EXISTS", .handler = command_exists}, // default EXISTS command

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

