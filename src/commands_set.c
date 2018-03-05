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

static size_t redis_set_handler_userkey(resp_request_t *request, index_root_t *index, data_root_t *data) {
    // create some easier accessor
    unsigned char *id = request->argv[1]->buffer;
    uint8_t idlength = request->argv[1]->length;

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

static size_t redis_set_handler_sequential(resp_request_t *request, index_root_t *index, data_root_t *data) {
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

static size_t redis_set_handler_directkey(resp_request_t *request, index_root_t *index, data_root_t *data) {
    // create some easier accessor
    index_dkey_t id = {
        .dataid = data_dataid(data),      // current data fileid
        .offset = data_next_offset(data), // needed now, we write it to the datafile
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

    // previously, we was skipping index at all on this mode
    // since there was no index, but now we use the index as statistics
    // manager, we use index, on the branch code, if there is no index in
    // memory, the memory part is skipped but index is still written
    if(!index_entry_insert(index, &id, idlength, offset, request->argv[2]->length)) {
        // cannot insert index (disk issue)
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    // building response
    // here, from original redis protocol, we don't reply with a basic
    // OK or Error when inserting a key, we reply with the key itself
    //
    // this is how the direct-id can returns the id generated
    redis_bulk_t response = redis_bulk(&id, idlength);
    if(!response.buffer) {
        redis_hardsend(request->client->fd, "$-1");
        return 0;
    }

    send(request->client->fd, response.buffer, response.length, 0);
    free(response.buffer);

    return offset;
}

static size_t (*redis_set_handlers[])(resp_request_t *request, index_root_t *index, data_root_t *data) = {
    redis_set_handler_userkey,    // key-value mode
    redis_set_handler_sequential, // incremental mode
    redis_set_handler_directkey,  // direct-key mode
};

int command_set(resp_request_t *request) {
    if(!command_args_validate(request, 3))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        redis_hardsend(request->client->fd, "-Key too large");
        return 1;
    }

    if(!request->client->writable) {
        debug("[-] command: set: denied, read-only namespace\n");
        redis_hardsend(request->client->fd, "-Namespace is in read-only mode");
        return 1;
    }

    // shortcut to data
    index_root_t *index = request->client->ns->index;
    data_root_t *data = request->client->ns->data;
    index_entry_t *entry = NULL;
    size_t floating = 0;

    // if the user want to override an existing key
    // and the maxsize of the namespace is reached, we need
    // to know if the replacement data is shorter, this is
    // a valid and legitimate insert request
    if((entry = redis_get_handlers[rootsettings.mode](request))) {
        floating = entry->length;
    }

    // check if namespace limitation is set
    if(request->client->ns->maxsize) {
        size_t limits = request->client->ns->maxsize + floating;

        // check if there is still enough space
        if(index->datasize + request->argv[2]->length > limits) {
            redis_hardsend(request->client->fd, "-No space left on this namespace");
            return 1;
        }
    }

    size_t offset = redis_set_handlers[rootsettings.mode](request, index, data);
    if(offset == 0)
        return 0;

    // checking if we need to jump to the next files
    // we do this check here and not from data (event if this is like a
    // datafile event) to keep data and index code completly distinct
    if(offset + request->argv[2]->length > DATA_MAXSIZE) {
        size_t newid = index_jump_next(request->client->ns->index);
        data_jump_next(request->client->ns->data, newid);
    }

    return 0;
}

