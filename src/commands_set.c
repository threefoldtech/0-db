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

static size_t redis_set_handler_userkey(redis_client_t *client) {
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
        redis_hardsend(client, "$-1");
        return 0;
    }

    debug("[+] command: set: userkey: ");
    debughex(id, idlength);
    debug("\n");

    debug("[+] command: set: offset: %lu\n", offset);

    // inserting this offset with the id on the index
    if(!index_entry_insert(index, id, idlength, offset, request->argv[2]->length)) {
        // cannot insert index (disk issue)
        redis_hardsend(client, "$-1");
        return 0;
    }

    // building response
    // here, from original redis protocol, we don't reply with a basic
    // OK or Error when inserting a key, we reply with the key itself
    //
    // this is how the sequential-id can returns the id generated
    redis_bulk_t response = redis_bulk(id, idlength);
    if(!response.buffer) {
        redis_hardsend(client, "$-1");
        return 0;
    }

    redis_reply(client, response.buffer, response.length, free);

    return offset;
}

static size_t redis_set_handler_sequential(redis_client_t *client) {
    resp_request_t *request = client->request;
    index_root_t *index = client->ns->index;
    data_root_t *data = client->ns->data;

    // create some easier accessor
    // grab the next id, this may be replaced
    // by user input if the key exists
    uint32_t id = index_next_id(index);
    uint8_t idlength = sizeof(uint32_t);

    if(request->argv[1]->length) {
        if(request->argv[1]->length != idlength) {
            debug("[-] redis: set: trying to insert key with invalid size\n");
            redis_hardsend(client, "-Invalid key, use empty key for auto-generated key");
            return 0;
        }

        index_entry_t *found = NULL;

        // looking for the requested key
        if(!(found = redis_get_handlers[SEQUENTIAL](client))) {
            debug("[-] redis: set: trying to insert invalid key\n");
            redis_hardsend(client, "-Invalid key, only update authorized");
            return 0;
        }

        memcpy(&id, found->id, idlength);
        debug("[+] redis: set: updating existing key: %08x\n", id);
    }

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
        redis_hardsend(client, "$-1");
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
        redis_hardsend(client, "$-1");
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
        redis_hardsend(client, "$-1");
        return 0;
    }

    redis_reply(client, response.buffer, response.length, free);

    return offset;
}

static size_t redis_set_handler_directkey(redis_client_t *client) {
    resp_request_t *request = client->request;
    index_root_t *index = client->ns->index;
    data_root_t *data = client->ns->data;

    // create some easier accessor
    uint8_t idlength = sizeof(index_dkey_t);
    index_dkey_t id = {
        .indexid = index_indexid(index),        // current index fileid
        .objectid = index_next_objectid(index), // needed now, it's part of the id
    };

    unsigned char *value = request->argv[2]->buffer;
    uint32_t valuelength = request->argv[2]->length;

    debug("[+] command: set: %u bytes key, %u bytes data\n", idlength, valuelength);

    // insert the data on the datafile
    // this will returns us the offset where the header is
    size_t offset = data_insert(data, value, valuelength, &id, idlength);

    // checking for writing error
    // if we couldn't write the data, we won't add entry on the index
    // and report to the client an error
    if(offset == 0) {
        redis_hardsend(client, "$-1");
        return 0;
    }

    debug("[+] command: set: direct-key: ");
    debughex(&id, idlength);
    debug("\n");

    debug("[+] command: set: offset: %lu\n", offset);

    // previously, we was skipping index at all on this mode
    // since there was no index, but now we use the index as statistics
    // manager, we use index, on the branch code, if there is no index in
    // memory, the memory part is skipped but index is still written
    if(!index_entry_insert(index, &id, idlength, offset, request->argv[2]->length)) {
        // cannot insert index (disk issue)
        redis_hardsend(client, "$-1");
        return 0;
    }

    // building response
    // here, from original redis protocol, we don't reply with a basic
    // OK or Error when inserting a key, we reply with the key itself
    //
    // this is how the direct-id can returns the id generated
    redis_bulk_t response = redis_bulk(&id, idlength);
    if(!response.buffer) {
        redis_hardsend(client, "$-1");
        return 0;
    }

    redis_reply(client, response.buffer, response.length, free);

    return offset;
}

static size_t (*redis_set_handlers[])(redis_client_t *client) = {
    redis_set_handler_userkey,    // key-value mode
    redis_set_handler_sequential, // incremental mode
    redis_set_handler_directkey,  // direct-key mode
    redis_set_handler_directkey,  // fixed blocks mode
};

int command_set(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(!command_args_validate_null(client, 3))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        redis_hardsend(client, "-Key too large");
        return 1;
    }

    if(!client->writable) {
        debug("[-] command: set: denied, read-only namespace\n");
        redis_hardsend(client, "-Namespace is in read-only mode");
        return 1;
    }

    // shortcut to data
    index_root_t *index = client->ns->index;
    index_entry_t *entry = NULL;
    size_t floating = 0;

    // if the user want to override an existing key
    // and the maxsize of the namespace is reached, we need
    // to know if the replacement data is shorter, this is
    // a valid and legitimate insert request
    if(request->argv[1]->length) {
        // userkey id is not null
        if((entry = redis_get_handlers[rootsettings.mode](client))) {
            floating = entry->length;
        }
    }

    // check if namespace limitation is set
    if(client->ns->maxsize) {
        size_t limits = client->ns->maxsize + floating;

        // check if there is still enough space
        if(index->datasize + request->argv[2]->length > limits) {
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
    if(data_next_offset(client->ns->data) + request->argv[2]->length > rootsettings.datasize) {
        size_t newid = index_jump_next(client->ns->index);
        data_jump_next(client->ns->data, newid);
    }

    size_t offset = redis_set_handlers[rootsettings.mode](client);
    if(offset == 0)
        return 0;

    return 0;
}

