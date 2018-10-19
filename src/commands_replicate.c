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
#include "commands_replicate.h"
#include "filesystem.h"

#define BUFFER_LENGTH 64
#define RESPONSE_LENGTH 2048

int command_mirror(redis_client_t *client) {
    if(!command_admin_authorized(client))
        return 1;

    client->mirror = 1;
    redis_hardsend(client, "+Starting mirroring");

    return 0;
}

int command_master(redis_client_t *client) {
    if(!command_admin_authorized(client))
        return 1;

    client->master = 1;
    redis_hardsend(client, "+Hello, master");

    return 0;
}


//
// IMPORT / EXPORT
//
// basically this is just a raw file read/write
// we just read specific file at specific offset and
// transfert the content over the wire, I think this is
// the most efficient way to transfert a complete namespace
// with full history etc. without dealing with lot of commands
// and complexity

static int command_export_descriptor(redis_client_t *client) {
    char response[RESPONSE_LENGTH];
    char filename[256], temp[BUFFER_LENGTH];
    filebuf_t *buffer;

    snprintf(filename, sizeof(filename), "%s/zdb-namespace", client->ns->index->indexdir);

    if(!(buffer = file_dump(filename, 0, 1024 * 1024))) {
        redis_hardsend(client, "-Internal Server Error");
        return 1;
    }

    sprintf(temp, "%ld", buffer->nextoff);
    sprintf(response, "*2\r\n$%ld\r\n", buffer->length);

    redis_reply_stack(client, response, strlen(response));
    redis_reply_heap(client, buffer->buffer, buffer->length, free);
    free(buffer);

    sprintf(response, "\r\n$%lu\r\n%s\r\n", strlen(temp), temp);
    redis_reply_stack(client, response, strlen(response));

    return 0;
}

static int command_export_indexdata(redis_client_t *client, char *filename, off_t offset) {
    char response[RESPONSE_LENGTH];
    char temp[BUFFER_LENGTH];
    filebuf_t *buffer;

    if(!(buffer = file_dump(filename, offset, 8 * 1024 * 1024))) {
        redis_hardsend(client, "-Internal Server Error");
        return 1;
    }

    sprintf(temp, "%ld", buffer->nextoff);
    sprintf(response, "*2\r\n$%ld\r\n", buffer->length);

    redis_reply_stack(client, response, strlen(response));
    redis_reply_heap(client, buffer->buffer, buffer->length, free);
    free(buffer);

    sprintf(response, "\r\n$%lu\r\n%s\r\n", strlen(temp), temp);
    redis_reply_stack(client, response, strlen(response));

    return 0;
}


static int command_export_index(redis_client_t *client, size_t fileid, off_t offset) {
    char filename[256];

    snprintf(filename, sizeof(filename), "%s/zdb-index-%05lu", client->ns->index->indexdir, fileid);
    return command_export_indexdata(client, filename, offset);
}

static int command_export_data(redis_client_t *client, size_t fileid, off_t offset) {
    char filename[256];

    snprintf(filename, sizeof(filename), "%s/zdb-data-%05lu", client->ns->data->datadir, fileid);
    return command_export_indexdata(client, filename, offset);
}



int command_export(redis_client_t *client) {
    resp_request_t *request = client->request;
    char object[BUFFER_LENGTH];
    char fileid[BUFFER_LENGTH];
    char offset[BUFFER_LENGTH];

    if(!command_admin_authorized(client))
        return 1;

    if(!command_args_validate(client, 4))
        return 1;

    // avoid overflow
    for(int i = 0; i < 4; i++) {
        if(request->argv[i]->length >= BUFFER_LENGTH) {
            redis_hardsend(client, "-Invalid argument");
            return 1;
        }
    }

    sprintf(object, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);
    sprintf(fileid, "%.*s", request->argv[2]->length, (char *) request->argv[2]->buffer);
    sprintf(offset, "%.*s", request->argv[3]->length, (char *) request->argv[3]->buffer);

    if(strcasecmp(object, "descriptor") == 0)
        return command_export_descriptor(client);

    if(strcasecmp(object, "index") == 0) {
        size_t fileidx = strtoul(fileid, NULL, 10);
        off_t fileoff = atol(offset);

        return command_export_index(client, fileidx, fileoff);
    }

    if(strcasecmp(object, "data") == 0) {
        size_t fileidx = strtoul(fileid, NULL, 10);
        off_t fileoff = atol(offset);

        return command_export_data(client, fileidx, fileoff);
    }

    redis_hardsend(client, "-Invalid object request");
    return 1;
}

int command_import(redis_client_t *client) {
    // resp_request_t *request = client->request;

    if(!command_admin_authorized(client))
        return 1;

    if(!command_args_validate(client, 2))
        return 1;

    redis_hardsend(client, "-Not implemented yet");

    return 0;
}
