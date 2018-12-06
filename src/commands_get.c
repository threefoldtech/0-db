#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <inttypes.h>
#include "index.h"
#include "zerodb.h"
#include "index_seq.h"
#include "index_get.h"
#include "data.h"
#include "namespace.h"
#include "redis.h"
#include "commands.h"

int command_get(redis_client_t *client) {
    resp_request_t *request = client->request;
    index_entry_t *entry = NULL;

    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        debug("[-] command: get: invalid key size (too big)\n");
        redis_hardsend(client, "-Invalid key");
        return 1;
    }

    // fetching index entry for this key
    if(!(entry = index_get(client->ns->index, request->argv[1]->buffer, request->argv[1]->length))) {
        debug("[-] command: get: key not found\n");
        redis_hardsend(client, "$-1");
        return 1;
    }

    // key found but deleted
    if(entry->flags & INDEX_ENTRY_DELETED) {
        verbose("[-] command: get: key deleted\n");
        redis_hardsend(client, "$-1");
        return 1;
    }

    // key found and valid, let's checking the contents
    debug("[+] command: get: entry found, flags: %x, data length: %" PRIu32 "\n", entry->flags, entry->length);
    debug("[+] command: get: data file: %d, data offset: %" PRIu32 "\n", entry->dataid, entry->offset);

    data_root_t *data = client->ns->data;
    data_payload_t payload = data_get(data, entry->offset, entry->length, entry->dataid, entry->idlength);

    if(!payload.buffer) {
        printf("[-] command: get: cannot read payload\n");
        redis_hardsend(client, "-Resource temporarily unavailable");
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

