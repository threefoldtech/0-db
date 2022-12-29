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

time_t timestamp_from_set(resp_request_t *request, int field) {
    // no timestamp on request, setting current time
    if(request->argc == field)
        return time(NULL);

    // convert argument to string
    char *temp = strndup(request->argv[field]->buffer, request->argv[field]->length);
    time_t timestamp = atoll(temp);
    free(temp);

    zdbd_debug("[+] index: using custom timestamp: %ld\n", timestamp);

    return timestamp;
}

static size_t redis_set_handler_userkey(redis_client_t *client, index_entry_t *existing) {
    resp_request_t *request = client->request;
    index_root_t *index = client->ns->index;
    data_root_t *data = client->ns->data;

    // create some easier accessor
    unsigned char *id = request->argv[1]->buffer;
    uint8_t idlength = request->argv[1]->length;

    if(idlength == 0) {
        redis_hardsend(client, "-Invalid argument, key needed");
        return 0;
    }

    unsigned char *value = request->argv[2]->buffer;
    uint32_t valuelength = request->argv[2]->length;

    // setting the timestamp
    time_t timestamp = timestamp_from_set(request, 3);

    zdbd_debug("[+] command: set: %u bytes key, %u bytes data\n", idlength, valuelength);
    // printf("[+] set key: %.*s\n", idlength, id);
    // printf("[+] set value: %.*s\n", request->argv[2]->length, (char *) request->argv[2]->buffer);

    data_request_t dreq = {
        .data = value,
        .datalength = valuelength,
        .vid = id,
        .idlength = idlength,
        .flags = 0,
        .crc = zdb_crc32(value, valuelength),
        .timestamp = timestamp,
    };

    // checking if we need to update this entry of if data are unchanged
    if(existing && existing->crc == dreq.crc) {
        zdbd_debug("[+] command: set: existing %08x <> %08x crc match, ignoring\n", existing->crc, dreq.crc);
        redis_hardsend(client, "$-1");
        return 0;
    }

    // insert the data into the datafile
    // this will return the offset where the header is located
    size_t offset = data_insert(data, &dreq);

    // check for write error
    // if we couldn't write the data, we won't add the entry on the index
    // and report to the client an error
    if(offset == 0) {
        redis_hardsend(client, "-Cannot write data right now");
        return 0;
    }

    zdbd_debug("[+] command: set: userkey: ");
    zdbd_debughex(id, idlength);

    zdbd_debug("[+] command: set: offset: %lu\n", offset);

    index_entry_t idxreq = {
        .idlength = idlength,
        .offset = offset,
        .length = request->argv[2]->length,
        .crc = dreq.crc,
        // .dataid = data->dataid,
        .flags = 0,
        .timestamp = timestamp,
    };

    index_set_t setter = {
        .entry = &idxreq,
        .id = id,
    };

    if(!index_set(index, &setter, existing)) {
        // cannot insert index (disk issue)
        redis_hardsend(client, "-Cannot write index right now");
        return 0;
    }

    // building response
    // here, like the original redis protocol, we don't reply with a basic
    // OK or Error when inserting a key, but we reply with the key itself
    //
    // this is how the sequential-id can return the generated id
    redis_bulk_t response = redis_bulk(id, idlength);
    if(!response.buffer) {
        redis_hardsend(client, "$-1");
        return 0;
    }

    redis_reply_heap(client, response.buffer, response.length, free);

    return offset;
}

static size_t redis_set_handler_sequential(redis_client_t *client, index_entry_t *existing) {
    resp_request_t *request = client->request;
    index_root_t *index = client->ns->index;
    data_root_t *data = client->ns->data;
    index_entry_t *idxentry = NULL;

    // if user provided a key and existing is not set
    // this means the key was not found, and we cannot update
    // it (obviously)
    if(request->argv[1]->length && !existing) {
        redis_hardsend(client, "-Invalid key, only update authorized");
        return 0;
    }

    // create some easier accessors
    // grab the next id, this may be replaced
    // by user input if the key exists
    seqid_t id = index_next_id(index);
    uint8_t idlength = sizeof(seqid_t);

    // setting key to existing if we do an update
    if(existing) {
        zdbd_debug("[+] command: set: updating existing id: ");
        zdbd_debughex(existing->id, existing->idlength);
        memcpy(&id, existing->id, existing->idlength);
    }

    unsigned char *value = request->argv[2]->buffer;
    uint32_t valuelength = request->argv[2]->length;

    // setting the timestamp
    time_t timestamp = timestamp_from_set(request, 3);

    zdbd_debug("[+] command: set: sequential-key: ");
    zdbd_debughex(&id, idlength);
    zdbd_debug("[+] command: set: %u bytes data\n", valuelength);

    data_request_t dreq = {
        .data = value,
        .datalength = valuelength,
        .vid = &id,
        .idlength = idlength,
        .flags = 0,
        .crc = zdb_crc32(value, valuelength),
        .timestamp = timestamp,
    };

    // checking if we need to update this entry or if data are unchanged
    if(existing && existing->crc == dreq.crc) {
        // it's possible to get same crc for similar data but
        // with different size, comparing size aswell
        if(existing->length == valuelength) {
            zdbd_debug("[+] command: set: existing %08x <> %08x crc match, ignoring\n", existing->crc, dreq.crc);
            redis_hardsend(client, "$-1");
            return 0;
        }
    }

    // insert the data into the datafile
    // this will return us the offset where the header is located
    // size_t offset = data_insert(value, valuelength, id, idlength);
    size_t offset = data_insert(data, &dreq);

    // checking for writing error
    // if we couldn't write the data, we won't add entry on the index
    // and report to the client an error
    if(offset == 0) {
        redis_hardsend(client, "-Cannot write data right now");
        return 0;
    }

    zdbd_debug("[+] command: set: writing data sequential-key: ");
    zdbd_debughex(&id, idlength);

    zdbd_debug("[+] command: set: offset: %lu\n", offset);

    index_entry_t idxreq = {
        .idlength = idlength,
        .offset = offset,
        .length = request->argv[2]->length,
        .crc = dreq.crc,
        .dataid = data->dataid,
        .flags = 0,
        .timestamp = timestamp,
    };

    index_set_t setter = {
        .entry = &idxreq,
        .id = &id,
    };

    if(!(idxentry = index_set(index, &setter, existing))) {
        // cannot insert index (disk issue)
        redis_hardsend(client, "-Cannot write index right now");
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
        redis_hardsend(client, "-Internal Error (bulk)");
        return 0;
    }

    redis_reply_heap(client, response.buffer, response.length, free);

    return offset;
}

static size_t (*redis_set_handlers[])(redis_client_t *client, index_entry_t *existing) = {
    redis_set_handler_userkey,    // key-value mode
    redis_set_handler_sequential, // incremental mode
    redis_set_handler_sequential, // direct-key mode (not used anymore)
    redis_set_handler_sequential, // fixed blocks mode (not implemented yet)
};

int command_set(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(request->argc == 4) {
        // we have a timestamp request
        // this is only authorized to admin users
        if(!command_admin_authorized(client))
            return 1;

    } else {
        // we don't have 4 argument, let's check
        // using default behavior we have 3 arguments
        if(!command_args_validate_null(client, 3))
            return 1;
    }

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        redis_hardsend(client, "-Key too large");
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

    // shortcut to data
    index_root_t *index = client->ns->index;
    index_entry_t *entry = NULL;
    index_mode_t mode = client->ns->index->mode;
    size_t floating = 0;

    // if the user want to override an existing key
    // and the maxsize of the namespace is reached, we need
    // to know if the replacement data is shorter, this is
    // a valid and legitimate insert request
    //
    // this make no sense in direct key mode, since we can't
    // update an existing key, we can only delete it
    if(request->argv[1]->length) {
        // userkey id is not null
        if((entry = index_get(index, request->argv[1]->buffer, request->argv[1]->length)))
            floating = entry->length;
    }

    // checking if worm mode enabled and key already exists
    if(entry && client->ns->worm) {
        zdbd_debug("[-] command: set: denied, overwriting an existing key with worm mode\n");
        redis_hardsend(client, "-Namespace is protected by worm mode");
        return 1;
    }

    // check if namespace limitation is set
    if(client->ns->maxsize) {
        size_t limits = client->ns->maxsize + floating;

        // check if there is still enough space
        if(index->stats.datasize + request->argv[2]->length > limits) {
            redis_hardsend(client, "-No space left on this namespace");
            return 1;
        }
    }

    // checking if we need to jump to the next files _before_ adding data
    // we do this check here and not from data (event if this is like a
    // datafile event) to keep data and index code completly distinct
    //
    // if we do this after adding data, we could have an empty data file
    // which will fake the 'previous' offset when computing it on reload
    zdb_settings_t *zdb_settings = zdb_settings_get();

    if(data_next_offset(client->ns->data) + request->argv[2]->length > zdb_settings->datasize) {
        size_t newid = index_jump_next(client->ns->index);
        data_jump_next(client->ns->data, newid);
    }

    size_t offset = redis_set_handlers[mode](client, entry);
    if(offset == 0)
        return 0;

    return 0;
}

