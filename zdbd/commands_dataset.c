#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <inttypes.h>
#include "libzdb.h"
#include "zdbd.h"
#include "redis.h"
#include "commands.h"
#include "commands_get.h"

int command_exists(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        zdb_log("[-] command: exists: invalid key size\n");
        redis_hardsend(client, "-Invalid key");
        return 1;
    }

    if(namespace_is_frozen(client->ns))
        return command_error_frozen(client);

    zdbd_debug("[+] command: exists: lookup key: ");
    zdbd_debughex(request->argv[1]->buffer, request->argv[1]->length);

    index_root_t *index = client->ns->index;
    index_entry_t *entry = index_get(index, request->argv[1]->buffer, request->argv[1]->length);

    zdbd_debug("[+] command: exists: entry found: %s\n", (entry ? "yes" : "no"));

    // key found but deleted
    if(entry && entry->flags & INDEX_ENTRY_DELETED) {
        zdbd_debug("[+] command: exists: entry found but deleted\n");
        entry = NULL;
    }

    int found = (entry == NULL) ? 0 : 1;

    char response[32];
    sprintf(response, ":%d\r\n", found);

    redis_reply_stack(client, response, strlen(response));

    return 0;
}

int command_check(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        zdb_log("[-] command: check: invalid key size\n");
        redis_hardsend(client, "-Invalid key");
        return 1;
    }

    if(namespace_is_frozen(client->ns))
        return command_error_frozen(client);

    zdbd_debug("[+] command: check: lookup key: ");
    zdbd_debughex(request->argv[1]->buffer, request->argv[1]->length);

    index_root_t *index = client->ns->index;
    index_entry_t *entry = index_get(index, request->argv[1]->buffer, request->argv[1]->length);

    // key not found at all
    if(!entry) {
        zdbd_debug("[-] command: check: key not found\n");
        redis_hardsend(client, "$-1");
        return 1;
    }

    // key found but deleted
    if(entry->flags & INDEX_ENTRY_DELETED) {
        zdbd_verbose("[-] command: check: key deleted\n");
        redis_hardsend(client, "$-1");
        return 1;
    }

    // key found and valid, let's checking the contents
    zdbd_debug("[+] command: get: entry found, flags: %x, data length: %" PRIu32 "\n", entry->flags, entry->length);
    zdbd_debug("[+] command: get: data file: %d, data offset: %" PRIu32 "\n", entry->dataid, entry->offset);

    data_root_t *data = client->ns->data;
    int status = data_check(data, entry->offset, entry->dataid);

    char response[32];
    sprintf(response, ":%d\r\n", status);

    redis_reply_stack(client, response, strlen(response));

    return 0;
}

int command_del(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(request->argc == 3) {
        // we have a timestamp request
        // this is only authorized to admin users
        if(!command_admin_authorized(client))
            return 1;

    } else {
        // we don't have 3 argument, let's check
        // using default behavior we have 2 arguments
        if(!command_args_validate(client, 2))
            return 1;
    }

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        zdb_log("[-] command: del: invalid key size\n");
        redis_hardsend(client, "-Invalid key");
        return 1;
    }

    if(!client->writable) {
        zdbd_debug("[-] command: set: denied, read-only namespace\n");
        redis_hardsend(client, "-Namespace is in read-only mode");
        return 1;
    }

    if(namespace_is_frozen(client->ns))
        return command_error_frozen(client);

    if(namespace_is_locked(client->ns))
        return command_error_locked(client);

    index_root_t *index = client->ns->index;
    data_root_t *data = client->ns->data;
    index_entry_t *entry;

    // grabbing original entry
    if(!(entry = index_get(index, request->argv[1]->buffer, request->argv[1]->length))) {
        zdbd_debug("[-] command: del: key not found\n");
        redis_hardsend(client, "-Key not found");
        return 1;
    }

    // avoid double deletion
    if(index_entry_is_deleted(entry)) {
        zdbd_debug("[-] command: del: key already deleted\n");
        redis_hardsend(client, "-Key not found");
        return 1;
    }

    // disable deletion when worm mode enabled
    if(client->ns->worm) {
        zdbd_debug("[-] command: del: denied, deleting a key with worm mode\n");
        redis_hardsend(client, "-Cannot delete a key when namespace is in worm mode");
        return 1;
    }

    time_t timestamp = timestamp_from_set(request, 2);

    // update data file, flag entry deleted
    if(!data_delete(data, entry->id, entry->idlength, timestamp)) {
        zdbd_debug("[-] command: del: deleting data failed\n");
        redis_hardsend(client, "-Cannot delete key (data)");
        return 0;
    }

    // mark index entry as deleted
    if(index_entry_delete(index, entry)) {
        zdbd_debug("[-] command: del: index delete flag failed\n");
        redis_hardsend(client, "-Cannot delete key (index)");
        return 0;
    }

    redis_hardsend(client, "+OK");

    return 0;
}

int command_length(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        zdb_log("[-] command: length: invalid key size\n");
        redis_hardsend(client, "-Invalid key");
        return 1;
    }

    if(namespace_is_frozen(client->ns))
        return command_error_frozen(client);

    zdbd_debug("[+] command: length: lookup key: ");
    zdbd_debughex(request->argv[1]->buffer, request->argv[1]->length);

    index_root_t *index = client->ns->index;
    index_entry_t *entry = index_get(index, request->argv[1]->buffer, request->argv[1]->length);

    // key not found at all
    if(!entry) {
        zdbd_debug("[-] command: length: key not found\n");
        redis_hardsend(client, "$-1");
        return 1;
    }

    // key found but deleted
    if(entry->flags & INDEX_ENTRY_DELETED) {
        zdbd_verbose("[-] command: length: key deleted\n");
        redis_hardsend(client, "$-1");
        return 1;
    }

    zdbd_debug("[+] command: length: entry found, flags: %x, data length: %" PRIu32 "\n", entry->flags, entry->length);

    char response[32];
    sprintf(response, ":%" PRIu32 "\r\n", entry->length);

    redis_reply_stack(client, response, strlen(response));

    return 0;
}

