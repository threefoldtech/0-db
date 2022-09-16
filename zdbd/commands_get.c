#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <inttypes.h>
#include "libzdb.h"
#include "index.h"
#include "zdbd.h"
#include "redis.h"
#include "commands.h"

static int command_get_single(redis_client_t *client, char *buffer, int length) {
    index_entry_t *entry = NULL;

    // fetching index entry for this key
    if(!(entry = index_get(client->ns->index, buffer, length))) {
        zdbd_debug("[-] command: get: key not found\n");
        redis_hardsend(client, "$-1");
        return 1;
    }

    // key found but deleted
    if(entry->flags & INDEX_ENTRY_DELETED) {
        zdbd_verbose("[-] command: get: key deleted\n");
        redis_hardsend(client, "$-1");
        return 1;
    }

    // key found and valid, let's check the contents
    zdbd_debug("[+] command: get: entry found, flags: %x, data length: %" PRIu32 "\n", entry->flags, entry->length);
    zdbd_debug("[+] command: get: data file: %d, data offset: %" PRIu32 "\n", entry->dataid, entry->offset);

    data_root_t *data = client->ns->data;
    data_payload_t payload = data_get(data, entry->offset, entry->length, entry->dataid, entry->idlength);

    if(!payload.buffer) {
        zdb_log("[-] command: get: cannot read payload\n");
        redis_hardsend(client, "-Internal Error");
        free(payload.buffer);
        return 0;
    }

    redis_bulk_t response = redis_bulk(payload.buffer, payload.length);
    if(!response.buffer) {
        redis_hardsend(client, "$-1");
        return 0;
    }

    redis_reply_heap(client, response.buffer, response.length, free);
    free(payload.buffer);

    return 0;

}

int command_get(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        zdbd_debug("[-] command: get: invalid key size (too big)\n");
        redis_hardsend(client, "-Invalid key");
        return 1;
    }

    if(namespace_is_frozen(client->ns))
        return command_error_frozen(client);

    return command_get_single(client, request->argv[1]->buffer, request->argv[1]->length);
}

int command_mget(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(client->request->argc < 2) {
        redis_hardsend(client, "-Invalid arguments");
        return 1;
    }

    // validating each keys
    for(int i = 1; i < client->request->argc; i++) {
        if(request->argv[i]->length > MAX_KEY_LENGTH) {
            zdbd_debug("[-] command: get: invalid key %d size (too big)\n", i);
            redis_hardsend(client, "-Invalid key");
            return 1;
        }
    }

    if(namespace_is_frozen(client->ns))
        return command_error_frozen(client);

    // streaming response to client
    char line[512];
    int length = client->request->argc - 1;

    sprintf(line, "*%d\r\n", length);
    redis_reply_stack(client, line, strlen(line));

    zdbd_debug("[+] command: mget: sending %d responses\n", length);

    for(int i = 1; i < request->argc; i++) {
        command_get_single(client, request->argv[i]->buffer, request->argv[i]->length);
    }

    return 0;
}

