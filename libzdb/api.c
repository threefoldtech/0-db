#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include "libzdb.h"
#include "libzdb_private.h"

static char *__zdb_api_types[] = {
    "ZDB_API_SUCCESS",
    "ZDB_API_FAILURE",
    "ZDB_API_ENTRY",
    "ZDB_API_UP_TO_DATE",
    "ZDB_API_BUFFER",
    "ZDB_API_NOT_FOUND",
    "ZDB_API_DELETED",
    "ZDB_API_INTERNAL_ERROR",
    "ZDB_API_TRUE",
    "ZDB_API_FALSE",
    "ZDB_API_INSERT_DENIED",
};

static_assert(
    sizeof(__zdb_api_types) / sizeof(char *) == ZDB_API_ITEMS_TOTAL,
    "zdb_api_types not inline with enum"
);

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
    // we don't need the key, we have the existing pointer
    // or we generate a new one
    (void) key;

    index_entry_t *idxentry = NULL;

    // if user provided a key and existing is not set
    // this means the key was not found, and we cannot update
    // it (obviously)
    if(ksize && !existing)
        return zdb_api_reply(ZDB_API_INSERT_DENIED, NULL);

    // create some easier accessor
    // grab the next id, this may be replaced
    // by user input if the key exists
    uint32_t id = index_next_id(ns->index);
    uint8_t idlength = sizeof(uint32_t);

    // setting key to existing if we do an update
    if(existing)
        memcpy(&id, existing->id, existing->idlength);

    // setting the timestamp // FIXME
    // time_t timestamp = timestamp_from_set(request);
    time_t timestamp = time(NULL);

    zdb_debug("[+] api: set: %u bytes key, %lu bytes data\n", idlength, psize);

    data_request_t dreq = {
        .data = payload,
        .datalength = psize,
        .vid = &id,
        .idlength = idlength,
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
    // size_t offset = data_insert(value, valuelength, id, idlength);
    size_t offset = data_insert(ns->data, &dreq);

    // checking for writing error
    // if we couldn't write the data, we won't add entry on the index
    // and report to the client an error
    if(offset == 0)
        return zdb_api_reply(ZDB_API_INTERNAL_ERROR, NULL);

    zdb_debug("[+] api: set: sequential-key: ");
    zdb_debughex(&id, idlength);
    zdb_debug("\n");

    zdb_debug("[+] api: set: offset: %lu\n", offset);

    index_entry_t idxreq = {
        .idlength = idlength,
        .offset = offset,
        .length = psize,
        .crc = dreq.crc,
        .flags = 0,
        .timestamp = timestamp,
    };

    index_set_t setter = {
        .entry = &idxreq,
        .id = &id,
    };

    if(!(idxentry = index_set(ns->index, &setter, existing))) {
        // cannot insert index (disk issue)
        return zdb_api_reply(ZDB_API_INTERNAL_ERROR, NULL);
    }

    // building response
    // here, from original redis protocol, we don't reply with a basic
    // OK or Error when inserting a key, we reply with the key itself
    //
    // this is how the sequential-id can returns the id generated
    // redis_bulk_t response = redis_bulk(id, idlength);
    return zdb_api_reply_buffer(&id, idlength);
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


//
// GET
//
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


//
// DATASET
//
zdb_api_t *zdb_api_exists(namespace_t *ns, void *key, size_t ksize) {
    index_entry_t *entry = index_get(ns->index, key, ksize);

    zdb_debug("[+] api: exists: entry found: %s\n", (entry ? "yes" : "no"));

    if(!entry)
        return zdb_api_reply(ZDB_API_FALSE, NULL);

    // key found but deleted
    if(entry && entry->flags & INDEX_ENTRY_DELETED) {
        zdb_debug("[+] api: exists: entry found but deleted\n");
        return zdb_api_reply(ZDB_API_FALSE, NULL);
    }

    return zdb_api_reply(ZDB_API_TRUE, NULL);
}

zdb_api_t *zdb_api_check(namespace_t *ns, void *key, size_t ksize) {
    index_entry_t *entry = index_get(ns->index, key, ksize);

    // key not found at all
    if(!entry) {
        zdb_debug("[-] api: check: key not found\n");
        return zdb_api_reply(ZDB_API_NOT_FOUND, NULL);
    }

    // key found but deleted
    if(entry->flags & INDEX_ENTRY_DELETED) {
        zdb_verbose("[-] api: check: key deleted\n");
        return zdb_api_reply(ZDB_API_DELETED, NULL);
    }

    // key found and valid, let's checking the contents
    zdb_debug("[+] api: get: entry found, flags: %x, data length: %" PRIu32 "\n", entry->flags, entry->length);
    zdb_debug("[+] api: get: data file: %d, data offset: %" PRIu32 "\n", entry->dataid, entry->offset);

    int status = data_check(ns->data, entry->offset, entry->dataid);
    return zdb_api_reply(status ? ZDB_API_TRUE : ZDB_API_FALSE, NULL);
}

zdb_api_t *zdb_api_del(namespace_t *ns, void *key, size_t ksize) {
    index_entry_t *entry;

    // grabbing original entry
    if(!(entry = index_get(ns->index, key, ksize))) {
        zdb_debug("[-] api: del: key not found\n");
        return zdb_api_reply(ZDB_API_NOT_FOUND, NULL);
    }

    // avoid double deletion
    if(index_entry_is_deleted(entry)) {
        zdb_debug("[-] api: del: key already deleted\n");
        return zdb_api_reply(ZDB_API_DELETED, NULL);
    }

    // update data file, flag entry deleted
    if(!data_delete(ns->data, entry->id, entry->idlength)) {
        zdb_debug("[-] api: del: deleting data failed\n");
        return zdb_api_reply(ZDB_API_INTERNAL_ERROR, NULL);
    }

    // mark index entry as deleted
    if(index_entry_delete(ns->index, entry)) {
        zdb_debug("[-] command: del: index delete flag failed\n");
        return zdb_api_reply(ZDB_API_INTERNAL_ERROR, NULL);
    }

    return zdb_api_reply_success();
}

index_root_t *zdb_index_init_lazy(zdb_settings_t *settings, char *indexdir, void *namespace) {
    return index_init_lazy(settings, indexdir, namespace);
}

index_root_t *zdb_index_init(zdb_settings_t *settings, char *indexdir, void *namespace, index_branch_t **branches) {
    return index_init(settings, indexdir, namespace, branches);
}

uint64_t zdb_index_availity_check(index_root_t *root) {
    return index_availity_check(root);
}

index_header_t *zdb_index_descriptor_load(index_root_t *root) {
    return index_descriptor_load(root);
}

index_header_t *zdb_index_descriptor_validate(index_header_t *header, index_root_t *root) {
    return index_descriptor_validate(header, root);
}

void zdb_index_set_id(index_root_t *root, uint64_t fileid) {
    return index_set_id(root, fileid);
}

int zdb_index_open_readonly(index_root_t *root, uint16_t fileid) {
    return index_open_readonly(root, fileid);
}

int zdb_index_open_readwrite(index_root_t *root, uint16_t fileid) {
    return index_open_readwrite(root, fileid);
}

uint64_t zdb_index_next_id(index_root_t *root) {
    return index_next_id(root);
}

index_item_t *zdb_index_raw_fetch_entry(index_root_t *root) {
    return index_raw_fetch_entry(root);
}

off_t zdb_index_raw_offset(index_root_t *root) {
    return lseek(root->indexfd, 0, SEEK_CUR);
}

void zdb_index_close(index_root_t *root) {
    index_close(root);
}

// expose internal crc32 computing
uint32_t zdb_checksum_crc32(const uint8_t *bytes, ssize_t length) {
    return data_crc32(bytes, length);
}

data_root_t *zdb_data_init_lazy(zdb_settings_t *settings, char *datapath, uint16_t dataid) {
    return data_init_lazy(settings, datapath, dataid);
}

int zdb_data_open_readonly(data_root_t *root) {
    return data_open_id_mode(root, root->dataid, O_RDONLY);
}

data_header_t *zdb_data_descriptor_load(data_root_t *root) {
    return data_descriptor_load(root);
}

data_header_t *zdb_data_descriptor_validate(data_header_t *header, data_root_t *root) {
    return data_descriptor_validate(header, root);
}
