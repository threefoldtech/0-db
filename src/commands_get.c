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

static index_entry_t *redis_get_handler_memkey(redis_client_t *client) {
    resp_request_t *request = client->request;
    index_root_t *index = client->ns->index;
    return index_entry_get(index, request->argv[1]->buffer, request->argv[1]->length);
}

static index_entry_t *redis_get_handler_direct(redis_client_t *client) {
    resp_request_t *request = client->request;

    // invalid requested key
    if(request->argv[1]->length != sizeof(index_dkey_t)) {
        debug("[-] command: get: invalid key length\n");
        return NULL;
    }

    // converting binary key to internal struct
    index_dkey_t directkey;

    memcpy(&directkey, request->argv[1]->buffer, sizeof(index_dkey_t));
    memcpy(index_reusable_entry->id, request->argv[1]->buffer, sizeof(index_dkey_t));

    index_reusable_entry->idlength = sizeof(index_dkey_t);
    index_reusable_entry->offset = directkey.offset;
    index_reusable_entry->dataid = directkey.dataid;
    index_reusable_entry->flags = 0;

    // since the user can provide any offset, he could potentially
    // get data not expected and maybe get sensitive data
    //
    // we don't have an exact way to ensure that this entry is effectivly
    // a valid entry, but we can be pretty sure
    //
    // the data header contains the key, if the offset points to one
    // header + the key just after, and that key match, we can conclude
    // the request is legitime
    //
    // sadly, this have some impact on read performance
    // FIXME: optimize this by changing when the security check is done
    //        but in the meantime, this fix the EXISTS command
    data_root_t *data = client->ns->data;
    size_t length;

    if(!(length = data_match(data, &directkey, sizeof(index_dkey_t), directkey.offset, directkey.dataid))) {
        debug("[-] command: get: validator refused the requested key access\n");
        return NULL;
    }

    index_reusable_entry->length = length;

    return index_reusable_entry;
}

index_entry_t * (*redis_get_handlers[])(redis_client_t *client) = {
    redis_get_handler_memkey, // key-value mode
    redis_get_handler_memkey, // incremental mode
    redis_get_handler_direct, // direct-key mode
    redis_get_handler_direct, // fixed block mode
};

int command_get(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        printf("[-] invalid key size\n");
        redis_hardsend(client, "-Invalid key");
        return 1;
    }

    debug("[+] command: get: lookup key: ");
    debughex(request->argv[1]->buffer, request->argv[1]->length);
    debug("\n");

    index_entry_t *entry = redis_get_handlers[rootsettings.mode](client);

    // key not found at all
    if(!entry) {
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
    debug("[+] command: get: entry found, flags: %x, data length: %" PRIu64 "\n", entry->flags, entry->length);
    debug("[+] command: get: data file: %d, data offset: %" PRIu64 "\n", entry->dataid, entry->offset);

    data_root_t *data = client->ns->data;
    data_payload_t payload = data_get(data, entry->offset, entry->length, entry->dataid, entry->idlength);

    if(!payload.buffer) {
        printf("[-] command: get: cannot read payload\n");
        redis_hardsend(client, "-Internal Error");
        free(payload.buffer);
        return 0;
    }

    redis_bulk_t response = redis_bulk(payload.buffer, payload.length);
    if(!response.buffer) {
        redis_hardsend(client, "$-1");
        return 0;
    }

    redis_reply(client, response.buffer, response.length, free);

    free(payload.buffer);

    return 0;
}

