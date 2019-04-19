#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include "libzdb.h"
#include "libzdb_private.h"

static char *__zdb_api_types[] = {
    "ZDB_API_SUCCESS",
    "ZDB_API_FAILURE",
    "ZDB_API_ENTRY",
    "ZDB_API_UP_TO_DATE",
    "ZDB_API_BUFFER"
};

//
// api response
//
static zdb_api_t *zdb_api_reply(zdb_api_type_t type, void *data) {
    zdb_api_t *response = malloc(sizeof(zdb_api_t));

    if(!response)
        zdb_diep("api: malloc");

    response->status = type;
    response->payload = data;

    return response;
}

static zdb_api_t *zdb_api_reply_success() {
    return zdb_api_reply(ZDB_API_SUCCESS, NULL);
}

static zdb_api_t *zdb_api_reply_error(char *error) {
    return zdb_api_reply(ZDB_API_FAILURE, strdup(error));
}

static zdb_api_t *zdb_api_reply_buffer(void *data, size_t size) {
    zdb_api_buffer_t *buffer = malloc(sizeof(zdb_api_buffer_t));

    buffer->size = size;
    buffer->payload = malloc(size);
    memcpy(buffer->payload, data, size);

    return zdb_api_reply(ZDB_API_BUFFER, buffer);
}

static zdb_api_t *zdb_api_reply_entry(void *key, size_t ksize, void *payload, size_t psize) {
    zdb_api_entry_t *entry = malloc(sizeof(zdb_api_entry_t));

    entry->key.size = ksize;
    entry->payload.size = psize;

    entry->key.payload = malloc(ksize);
    memcpy(entry->key.payload, key, ksize);

    // WARNING: memory not duplicated
    entry->payload.payload = payload;

    return zdb_api_reply(ZDB_API_ENTRY, entry);
}


void zdb_api_reply_free(zdb_api_t *reply) {
    if(reply->status == ZDB_API_FAILURE)
        free(reply->payload);

    if(reply->status == ZDB_API_BUFFER) {
        zdb_api_buffer_t *buffer = reply->payload;
        free(buffer->payload);
        free(buffer);
    }

    if(reply->status == ZDB_API_ENTRY) {
        zdb_api_entry_t *entry = reply->payload;
        free(entry->key.payload);
        free(entry->payload.payload);
        free(entry);
    }

    free(reply);
}

char *zdb_api_debug_type(zdb_api_type_t type) {
    return __zdb_api_types[type];
}

//
// api
//
static zdb_api_t *api_set_handler_userkey(namespace_t *ns, void *key, size_t ksize, void *payload, size_t psize, index_entry_t *existing) {
    if(ksize == 0)
        return zdb_api_reply_error("Invalid argument, key needed");

    // setting the timestamp // FIXME
    // time_t timestamp = timestamp_from_set(request);
    time_t timestamp = time(NULL);

    zdb_debug("[+] api: set: %lu bytes key, %lu bytes data\n", ksize, psize);

    data_request_t dreq = {
        .data = payload,
        .datalength = psize,
        .vid = key,
        .idlength = ksize,
        .flags = 0,
        .crc = data_crc32(payload, psize),
        .timestamp = timestamp,
    };

    // checking if we need to update this entry of if data are unchanged
    if(existing && existing->crc == dreq.crc) {
        zdb_debug("[+] api: set: existing %08x <> %08x crc match, ignoring\n", existing->crc, dreq.crc);
        return zdb_api_reply(ZDB_API_UP_TO_DATE, NULL);
    }

    // insert the data on the datafile
    // this will returns us the offset where the header is
    size_t offset = data_insert(ns->data, &dreq);

    // checking for writing error
    // if we couldn't write the data, we won't add entry on the index
    // and report to the client an error
    if(offset == 0)
        return zdb_api_reply_error("Cannot write data right now");

    zdb_debug("[+] api: set: userkey: ");
    zdb_debughex(key, ksize);
    zdb_debug("\n");

    zdb_debug("[+] api: set: offset: %lu\n", offset);

    index_entry_t idxreq = {
        .idlength = ksize,
        .offset = offset,
        .length = psize,
        .crc = dreq.crc,
        .flags = 0,
        .timestamp = timestamp,
    };

    index_set_t setter = {
        .entry = &idxreq,
        .id = key,
    };

    if(!index_set(ns->index, &setter, existing))
        return zdb_api_reply_error("Cannot write index right now");

    return zdb_api_reply_buffer(key, ksize);
}

static zdb_api_t *api_set_handler_sequential(namespace_t *ns, void *key, size_t ksize, void *payload, size_t psize, index_entry_t *existing) {
#if 0
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

    // create some easier accessor
    // grab the next id, this may be replaced
    // by user input if the key exists
    uint32_t id = index_next_id(index);
    uint8_t idlength = sizeof(uint32_t);

    // setting key to existing if we do an update
    if(existing)
        memcpy(&id, existing->id, existing->idlength);

    unsigned char *value = request->argv[2]->buffer;
    uint32_t valuelength = request->argv[2]->length;

    // setting the timestamp
    time_t timestamp = timestamp_from_set(request);

    zdbd_debug("[+] command: set: %u bytes key, %u bytes data\n", idlength, valuelength);

    data_request_t dreq = {
        .data = value,
        .datalength = valuelength,
        .vid = &id,
        .idlength = idlength,
        .flags = 0,
        .crc = data_crc32(value, valuelength),
        .timestamp = timestamp,
    };

    // checking if we need to update this entry of if data are unchanged
    if(existing && existing->crc == dreq.crc) {
        zdbd_debug("[+] command: set: existing %08x <> %08x crc match, ignoring\n", existing->crc, dreq.crc);
        redis_hardsend(client, "$-1");
        return 0;
    }

    // insert the data on the datafile
    // this will returns us the offset where the header is
    // size_t offset = data_insert(value, valuelength, id, idlength);
    size_t offset = data_insert(data, &dreq);

    // checking for writing error
    // if we couldn't write the data, we won't add entry on the index
    // and report to the client an error
    if(offset == 0) {
        redis_hardsend(client, "-Cannot write data right now");
        return 0;
    }

    zdbd_debug("[+] command: set: sequential-key: ");
    zdbd_debughex(&id, idlength);
    zdbd_debug("\n");

    zdbd_debug("[+] command: set: offset: %lu\n", offset);

    index_entry_t idxreq = {
        .idlength = idlength,
        .offset = offset,
        .length = request->argv[2]->length,
        .crc = dreq.crc,
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
#endif
    return NULL;
}



static zdb_api_t *(*api_set_handlers[])(namespace_t *ns, void *key, size_t ksize, void *payload, size_t psize, index_entry_t *existing) = {
    api_set_handler_userkey,    // key-value mode
    api_set_handler_sequential, // incremental mode
    api_set_handler_sequential, // direct-key mode (not used anymore)
    api_set_handler_sequential, // fixed blocks mode (not implemented yet)
};


zdb_api_t *zdb_api_set(namespace_t *ns, void *key, size_t ksize, void *payload, size_t psize) {
    index_entry_t *entry = NULL;
    size_t floating = 0;

    // if the user want to override an existing key
    // and the maxsize of the namespace is reached, we need
    // to know if the replacement data is shorter, this is
    // a valid and legitimate insert request
    //
    // this make no sense in direct key mode, since we can't
    // update an existing key, we can only delete it
    if(ksize) {
        // userkey id is not null
        if((entry = index_get(ns->index, key, ksize)))
            floating = entry->length;
    }

    // check if namespace limitation is set
    if(ns->maxsize) {
        size_t limits = ns->maxsize + floating;

        // check if there is still enough space
        if(ns->index->datasize + psize > limits)
            return zdb_api_reply_error("No space left on this namespace");
    }

    // checking if we need to jump to the next files _before_ adding data
    // we do this check here and not from data (event if this is like a
    // datafile event) to keep data and index code completly distinct
    //
    // if we do this after adding data, we could have an empty data file
    // which will fake the 'previous' offset when computing it on reload
    if(data_next_offset(ns->data) + psize > zdb_rootsettings.datasize) {
        size_t newid = index_jump_next(ns->index);
        data_jump_next(ns->data, newid);
    }

    return api_set_handlers[zdb_rootsettings.mode](ns, key, ksize, payload, psize, entry);
}








zdb_api_t *zdb_api_get(namespace_t *ns, void *key, size_t ksize) {
    index_entry_t *entry = NULL;

    // fetching index entry for this key
    if(!(entry = index_get(ns->index, key, ksize))) {
        zdb_debug("[-] api: get: key not found\n");
        return zdb_api_reply(ZDB_API_NOT_FOUND, NULL);
    }

    // key found but deleted
    if(entry->flags & INDEX_ENTRY_DELETED) {
        zdb_verbose("[-] api: get: key deleted\n");
        return zdb_api_reply(ZDB_API_DELETED, NULL);
    }

    // key found and valid, let's checking the contents
    zdb_debug("[+] api: get: entry found, flags: %x, data length: %" PRIu32 "\n", entry->flags, entry->length);
    zdb_debug("[+] api: get: data file: %d, data offset: %" PRIu32 "\n", entry->dataid, entry->offset);

    data_root_t *data = ns->data;
    data_payload_t payload = data_get(data, entry->offset, entry->length, entry->dataid, entry->idlength);

    if(!payload.buffer) {
        printf("[-] api: get: cannot read payload\n");
        free(payload.buffer);
        return zdb_api_reply(ZDB_API_INTERNAL_ERROR, NULL);
    }

    // WARNING: buffer is not duplicated when setting payload reply
    // it wil be free by zdb_api_reply_free later
    return zdb_api_reply_entry(key, ksize, payload.buffer, payload.length);
}
