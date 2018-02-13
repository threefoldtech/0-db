#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <inttypes.h>
#include "redis.h"
#include "sockets.h"
#include "zerodb.h"
#include "index.h"
#include "data.h"

static int yes = 1;

static void redis_bulk_append(redis_bulk_t *bulk, void *data, size_t length) {
    memcpy(bulk->buffer + bulk->writer, data, length);
    bulk->writer += length;
}

static redis_bulk_t redis_bulk(void *payload, size_t length) {
    unsigned char *buffer = (unsigned char *) payload;
    redis_bulk_t bulk = {
        .length = 0,
        .writer = 0,
        .buffer = NULL,
    };

    // convert length to string
    char strsize[64];
    size_t stroffset = sprintf(strsize, "%zu", length);

    // build response:
    // 1) $             -- bulk response
    // 2) (length-str)  -- response length (in string)
    // 3) \r\n          -- CRLF
    // 4) (payload)     -- binary payload
    // 5) \r\n          -- CRLF
    //
    bulk.length = 1 + stroffset + 2 + length + 2;
    if(!(bulk.buffer = malloc(bulk.length))) {
        warnp("bulk malloc");
        return bulk;
    }

    // build redis response
    redis_bulk_append(&bulk, "$", 1);
    redis_bulk_append(&bulk, strsize, stroffset);
    redis_bulk_append(&bulk, "\r\n", 2);
    redis_bulk_append(&bulk, buffer, length);
    redis_bulk_append(&bulk, "\r\n", 2);

    return bulk;
}

//
// different SET implementation
// depending on the running mode
//
size_t redis_set_handler_userkey(resp_request_t *request) {
    // create some easier accessor
    unsigned char *id = request->argv[1]->buffer;
    uint8_t idlength = request->argv[1]->length;

    unsigned char *value = request->argv[2]->buffer;
    uint32_t valuelength = request->argv[2]->length;

    debug("[+] set command: %u bytes key, %u bytes data\n", idlength, valuelength);
    // printf("[+] set key: %.*s\n", idlength, id);
    // printf("[+] set value: %.*s\n", request->argv[2]->length, (char *) request->argv[2]->buffer);

    // insert the data on the datafile
    // this will returns us the offset where the header is
    size_t offset = data_insert(value, valuelength, id, idlength);

    // checking for writing error
    // if we couldn't write the data, we won't add entry on the index
    // and report to the client an error
    if(offset == 0) {
        redis_hardsend(request->fd, "$-1");
        return 0;
    }

    debug("[+] userkey: ");
    debughex(id, idlength);
    debug("\n");

    debug("[+] data insertion offset: %lu\n", offset);

    // inserting this offset with the id on the index
    if(!index_entry_insert(id, idlength, offset, request->argv[2]->length)) {
        // cannot insert index (disk issue)
        redis_hardsend(request->fd, "$-1");
        return 0;
    }

    // building response
    // here, from original redis protocol, we don't reply with a basic
    // OK or Error when inserting a key, we reply with the key itself
    //
    // this is how the sequential-id can returns the id generated
    redis_bulk_t response = redis_bulk(id, idlength);
    if(!response.buffer) {
        redis_hardsend(request->fd, "$-1");
        return 0;
    }

    send(request->fd, response.buffer, response.length, 0);
    free(response.buffer);

    return offset;
}

size_t redis_set_handler_sequential(resp_request_t *request) {
    // create some easier accessor
    uint32_t id = index_next_id();
    uint8_t idlength = sizeof(uint32_t);

    unsigned char *value = request->argv[2]->buffer;
    uint32_t valuelength = request->argv[2]->length;

    debug("[+] set command: %u bytes key, %u bytes data\n", idlength, valuelength);
    // printf("[+] set key: %.*s\n", idlength, id);
    // printf("[+] set value: %.*s\n", request->argv[2]->length, (char *) request->argv[2]->buffer);

    // insert the data on the datafile
    // this will returns us the offset where the header is
    // size_t offset = data_insert(value, valuelength, id, idlength);
    size_t offset = data_insert(value, valuelength, &id, idlength);

    // checking for writing error
    // if we couldn't write the data, we won't add entry on the index
    // and report to the client an error
    if(offset == 0) {
        redis_hardsend(request->fd, "$-1");
        return 0;
    }

    debug("[+] generated key: ");
    debughex(&id, idlength);
    debug("\n");

    debug("[+] data insertion offset: %lu\n", offset);

    // inserting this offset with the id on the index
    // if(!index_entry_insert(id, idlength, offset, request->argv[2]->length)) {
    if(!index_entry_insert(&id, idlength, offset, request->argv[2]->length)) {
        // cannot insert index (disk issue)
        redis_hardsend(request->fd, "$-1");
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
        redis_hardsend(request->fd, "$-1");
        return 0;
    }

    send(request->fd, response.buffer, response.length, 0);
    free(response.buffer);

    return offset;
}

size_t redis_set_handler_test(resp_request_t *request) {
    (void) request;
    printf("NOOP\n");

    return 0;
}

size_t (*redis_set_handlers[])(resp_request_t *request) = {
    redis_set_handler_userkey,
    redis_set_handler_sequential,
    redis_set_handler_test
};

// main worker when a redis command was successfuly parsed
int redis_dispatcher(resp_request_t *request) {
    if(request->argv[0]->type != STRING) {
        debug("[+] not a string command, ignoring\n");
        return 0;
    }

    // PING
    if(!strncmp(request->argv[0]->buffer, "PING", request->argv[0]->length)) {
        verbose("[+] redis: PING\n");
        redis_hardsend(request->fd, "+PONG");
        return 0;
    }

    // SET
    if(!strncmp(request->argv[0]->buffer, "SET", request->argv[0]->length)) {
        if(request->argc != 3) {
            redis_hardsend(request->fd, "-Invalid argument");
            return 1;
        }

        if(request->argv[1]->length > MAX_KEY_LENGTH) {
            redis_hardsend(request->fd, "-Key too large");
            return 1;
        }

        size_t offset = redis_set_handlers[rootsettings.mode](request);
        if(offset == 0)
            return 0;

#if 0
        // create some easier accessor
        unsigned char *id = request->argv[1]->buffer;
        uint8_t idlength = request->argv[1]->length;

        unsigned char *value = request->argv[2]->buffer;
        uint32_t valuelength = request->argv[2]->length;

        idlength = sizeof(uint32_t);
        uint32_t thisid = index_next_id();

        debug("[+] set command: %u bytes key, %u bytes data\n", idlength, valuelength);
        // printf("[+] set key: %.*s\n", idlength, id);
        // printf("[+] set value: %.*s\n", request->argv[2]->length, (char *) request->argv[2]->buffer);

        // insert the data on the datafile
        // this will returns us the offset where the header is
        // size_t offset = data_insert(value, valuelength, id, idlength);
        size_t offset = data_insert(value, valuelength, &thisid, idlength);

        // checking for writing error
        // if we couldn't write the data, we won't add entry on the index
        // and report to the client an error
        if(offset == 0) {
            redis_hardsend(request->fd, "$-1");
            return 0;
        }

        debug("[+] userkey: ");
        // debughex(id, idlength);
        debughex(&thisid, idlength);
        debug("\n");

        debug("[+] data insertion offset: %lu\n", offset);

        // inserting this offset with the id on the index
        // if(!index_entry_insert(id, idlength, offset, request->argv[2]->length)) {
        if(!index_entry_insert(&thisid, idlength, offset, request->argv[2]->length)) {
            // cannot insert index (disk issue)
            redis_hardsend(request->fd, "$-1");
            return 0;
        }

        // building response
        // here, from original redis protocol, we don't reply with a basic
        // OK or Error when inserting a key, we reply with the key itself
        //
        // this is how the sequential-id can returns the id generated
        // redis_bulk_t response = redis_bulk(id, idlength);
        redis_bulk_t response = redis_bulk(&thisid, idlength);
        if(!response.buffer) {
            redis_hardsend(request->fd, "$-1");
            return 0;
        }

        send(request->fd, response.buffer, response.length, 0);
        free(response.buffer);
#endif

        // checking if we need to jump to the next files
        // we do this check here and not from data (event if this is like a
        // datafile event) to keep data and index code completly distinct
        if(offset + request->argv[2]->length > 256 * 1024 * 1024) { // 256 MB
            size_t newid = index_jump_next();
            data_jump_next(newid);
        }

        return 0;
    }

    // GET
    if(!strncmp(request->argv[0]->buffer, "GET", request->argv[0]->length)) {
        if(request->argv[1]->length > MAX_KEY_LENGTH) {
            printf("[-] invalid key size\n");
            redis_hardsend(request->fd, "-Invalid key");
            return 1;
        }

        debug("[+] lookup key: ");
        debughex(request->argv[1]->buffer, request->argv[1]->length);
        debug("\n");

        index_entry_t *entry = index_entry_get(request->argv[1]->buffer, request->argv[1]->length);

        // key not found at all
        if(!entry) {
            verbose("[-] key not found\n");
            redis_hardsend(request->fd, "$-1");
            return 1;
        }

        // key found but deleted
        if(entry->flags & INDEX_ENTRY_DELETED) {
            verbose("[-] key deleted\n");
            redis_hardsend(request->fd, "$-1");
            return 1;
        }

        // key found and valid, let's checking the contents
        debug("[+] entry found, flags: %x, data length: %" PRIu64 "\n", entry->flags, entry->length);
        debug("[+] data file: %d, data offset: %" PRIu64 "\n", entry->dataid, entry->offset);

        unsigned char *payload = data_get(entry->offset, entry->length, entry->dataid, entry->idlength);

        if(!payload) {
            printf("[-] cannot read payload\n");
            redis_hardsend(request->fd, "-Internal Error");
            free(payload);
            return 0;
        }

        redis_bulk_t response = redis_bulk(payload, entry->length);
        if(!response.buffer) {
            redis_hardsend(request->fd, "$-1");
            return 0;
        }

        send(request->fd, response.buffer, response.length, 0);

        free(response.buffer);
        free(payload);

        return 0;
    }

    if(!strncmp(request->argv[0]->buffer, "DEL", request->argv[0]->length)) {
        if(request->argv[1]->length > MAX_KEY_LENGTH) {
            printf("[-] invalid key size\n");
            redis_hardsend(request->fd, "-Invalid key");
            return 1;
        }

        if(!index_entry_delete(request->argv[1]->buffer, request->argv[1]->length)) {
            redis_hardsend(request->fd, "-Cannot delete key");
            return 0;
        }

        redis_hardsend(request->fd, "+OK");
        return 0;
    }

    #ifndef RELEASE
    // STOP will be only compiled in debug mode
    // this will force to exit listen loop in order to call
    // all destructors, this is useful to ensure every memory allocation
    // are well tracked and well cleaned
    //
    // in production, a user should not be able to stop the daemon
    if(!strncmp(request->argv[0]->buffer, "STOP", request->argv[0]->length)) {
        redis_hardsend(request->fd, "+Stopping");
        return 2;
    }
    #endif

    // unknown
    printf("[-] unsupported redis command\n");
    redis_hardsend(request->fd, "-Command not supported");

    return 1;
}

static size_t recvto(int fd, char *buffer, size_t length) {
    size_t remain = length;
    size_t shot = 0;
    char *writer = buffer;

    while(remain && (shot = recv(fd, writer, remain, 0)) > 0) {
        remain -= shot;
        writer += shot;
    }

    return length;
}

static void resp_discard(int fd, char *message) {
    char response[512];

    sprintf(response, "-%s\r\n", message);
    fprintf(stderr, "[-] error: %s\n", message);

    if(send(fd, response, strlen(response), 0) < 0)
        fprintf(stderr, "[-] send failed for error message\n");
}

// parsing a redis request
// we parse the string and try to allocate something
// in memory representing the request
//
// basic kind of argc/argv is used to expose the received request
int redis_response(int fd) {
    resp_request_t command;

    char buffer[8192], *reader = NULL;
    int length;
    int dvalue = 0;

    command.fd = fd;

    while((length = recv(fd, buffer, sizeof(buffer) - 1, 0)) > 0) {
        // array
        if(buffer[0] != '*') {
            printf("[-] not an array\n");
            return 0;
        }

        if(!(reader = strchr(buffer, '\n'))) {
            // error.
            return 0;
        }

        command.argc = atoi(buffer + 1);
        // printf("[+] resp: %d arguments\n", command.argc);

        if(!(reader = strchr(buffer, '\n')))
            return 0;

        reader += 1;
        command.argv = (resp_object_t **) calloc(sizeof(resp_type_t *), command.argc);

        for(int i = 0; i < command.argc; i++) {
            command.argv[i] = malloc(sizeof(resp_object_t));
            resp_object_t *argument = command.argv[i];

            // reading next chunk
            // verifiying it's a string command
            if(*reader != '$') {
                resp_discard(command.fd, "Malformed request (string)");
                goto cleanup;
            }

            argument->type = STRING;
            argument->length = atoi(reader + 1);
            // printf("[+] string len: %d\n", argument->length);

            // going to the next line for the payload
            if(!(reader = strchr(reader, '\n'))) {
                resp_discard(command.fd, "Malformed request (payload)");
                goto cleanup;
            }

            if(argument->length > 8 * 1024 * 1024) {
                resp_discard(command.fd, "Payload too big");
                goto cleanup;
            }

            if(!(argument->buffer = malloc(argument->length)))
                diep("malloc");

            size_t remain = length - (reader + 1 - buffer);

            // reading the payload
            if(remain >= (size_t) argument->length) {
                // we have enough data on the buffer, we can just take
                // what we needs
                memcpy(argument->buffer, reader + 1, argument->length);
                reader += argument->length + 3; // previous \n + \r\n at the end
                continue;
            }

            // the buffer doesn't contains enough data, let's read again to fill in the data
            memcpy(argument->buffer, reader + 1, remain);
            recvto(command.fd, argument->buffer + remain, argument->length - remain);

            if(recv(command.fd, buffer, 2, 0) != 2) {
                fprintf(stderr, "[-] recv failed, ignoring\n");
                goto cleanup;
            }

            // we read the rest of the payload, the rest on the socket should
            // be the end-of-line of the redis protocol
            if(strncmp(buffer, "\r\n", 2)) {
                resp_discard(command.fd, "Protocol malformed");
                goto cleanup;
            }
        }

        dvalue = redis_dispatcher(&command);

cleanup:
        for(int i = 0; i < command.argc; i++) {
            free(command.argv[i]->buffer);
            free(command.argv[i]);
        }

        free(command.argv);

        // special catch for STOP request
        if(dvalue == 2)
            return 2;

        return 0;
    }

    // error, let's drop this client
    return 3;
}

void socket_nonblock(int fd) {
    int flags;

    if((flags = fcntl(fd, F_GETFL, 0)) < 0)
        diep("fcntl");

    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void socket_block(int fd) {
    int flags;

    if((flags = fcntl(fd, F_GETFL, 0)) < 0)
        diep("fcntl");

    fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
}

int redis_listen(char *listenaddr, int port) {
    struct sockaddr_in addr_listen;
    redis_handler_t redis;

    // classic basic network socket operation
    addr_listen.sin_family = AF_INET;
    addr_listen.sin_port = htons(port);
    addr_listen.sin_addr.s_addr = inet_addr(listenaddr);

    if((redis.mainfd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
        diep("socket");

    if(setsockopt(redis.mainfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1)
        diep("setsockopt");

    socket_nonblock(redis.mainfd);

    if(bind(redis.mainfd, (struct sockaddr*) &addr_listen, sizeof(addr_listen)) == -1)
        diep("bind");

    if(listen(redis.mainfd, SOMAXCONN) == -1)
        diep("listen");

    success("[+] listening on %s:%d", listenaddr, port);

    // entering the worker loop
    return socket_handler(&redis);
}
