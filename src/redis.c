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
#include <time.h>
#include <inttypes.h>
#include "sockets.h"
#include "zerodb.h"
#include "index.h"
#include "data.h"
#include "namespace.h"
#include "redis.h"
#include "commands.h"

static int yes = 1;

// list of active clients
static redis_clients_t clients = {
    .length = 0,
    .list = NULL,
};

void redis_bulk_append(redis_bulk_t *bulk, void *data, size_t length) {
    memcpy(bulk->buffer + bulk->writer, data, length);
    bulk->writer += length;
}

redis_bulk_t redis_bulk(void *payload, size_t length) {
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
    debug("[-] redis: resp: error: %s\n", message);

    if(send(fd, response, strlen(response), 0) < 0)
        fprintf(stderr, "[-] send failed for error message\n");
}

// parsing a redis request
// we parse the string and try to allocate something
// in memory representing the request
//
// basic kind of argc/argv is used to expose the received request
int redis_response(int fd) {
    resp_request_t command = {
        .argc = 0,
        .argv = NULL
    };

    char buffer[8192], *reader = NULL;
    int length;
    int dvalue = 0;

    command.client = clients.list[fd];

    while((length = recv(fd, buffer, sizeof(buffer) - 1, 0)) > 0) {
        // array
        if(buffer[0] != '*') {
            resp_discard(command.client->fd, "Malformed request, array expected");
            return 0;
        }

        if(!(reader = strchr(buffer, '\n'))) {
            // error
            return 0;
        }

        command.argc = atoi(buffer + 1);
        debug("[+] redis: resp: %d arguments\n", command.argc);

        if(command.argc == 0) {
            resp_discard(command.client->fd, "Missing arguments");
            return 0;
        }

        if(command.argc > 16) {
            resp_discard(command.client->fd, "Too many arguments");
            return 0;
        }

        if(!(reader = strchr(buffer, '\n')))
            return 0;

        reader += 1;
        command.argv = (resp_object_t **) calloc(sizeof(resp_type_t *), command.argc);

        for(int i = 0; i < command.argc; i++) {
            command.argv[i] = calloc(sizeof(resp_object_t), 1);
            resp_object_t *argument = command.argv[i];

            // reading next chunk
            // verifiying it's a string command
            if(*reader != '$') {
                resp_discard(command.client->fd, "Malformed request (string)");
                goto cleanup;
            }

            argument->type = STRING;
            argument->length = atoi(reader + 1);
            // printf("[+] string len: %d\n", argument->length);

            // going to the next line for the payload
            if(!(reader = strchr(reader, '\n'))) {
                resp_discard(command.client->fd, "Malformed request (payload)");
                goto cleanup;
            }

            if(argument->length > 8 * 1024 * 1024) {
                resp_discard(command.client->fd, "Payload too big");
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
            recvto(command.client->fd, argument->buffer + remain, argument->length - remain);

            if(recv(command.client->fd, buffer, 2, 0) != 2) {
                fprintf(stderr, "[-] recv failed, ignoring\n");
                goto cleanup;
            }

            // we read the rest of the payload, the rest on the socket should
            // be the end-of-line of the redis protocol
            if(strncmp(buffer, "\r\n", 2)) {
                resp_discard(command.client->fd, "Protocol malformed");
                goto cleanup;
            }
        }

        dvalue = redis_dispatcher(&command);

cleanup:
        for(int i = 0; i < command.argc; i++) {
            // prematured end and argv was not yet allocated
            if(!command.argv[i])
                continue;

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

// allocate a new client for a new file descriptor
// used to keep session-life information about clients
redis_client_t *socket_client_new(int fd) {
    debug("[+] new client (fd: %d)\n", fd);

    if(fd >= (int) clients.length) {
        redis_client_t **newlist = NULL;
        size_t newlength = clients.length + fd;

        // growing up the list
        if(!(newlist = (redis_client_t **) realloc(clients.list, sizeof(redis_client_t *) * newlength)))
            return NULL;

        // ensure new clients are not set
        for(size_t i = clients.length; i < newlength; i++)
            newlist[i] = NULL;

        // increase clients list
        clients.list = newlist;
        clients.length = newlength;
    }

    clients.list[fd] = malloc(sizeof(redis_client_t));
    clients.list[fd]->fd = fd;
    clients.list[fd]->connected = time(NULL);
    clients.list[fd]->commands = 0;

    // attaching default namespace to this client
    clients.list[fd]->ns = namespace_get_default();

    // by default, the default namespace is writable
    clients.list[fd]->writable = 1;

    // set all users admin if no password are set
    clients.list[fd]->admin = (rootsettings.adminpwd) ? 0 : 1;

    return clients.list[fd];
}

// free allocated client when disconnected
void socket_client_free(int fd) {
    redis_client_t *client = clients.list[fd];

    debug("[+] client: closing (fd: %d)\n", fd);

    #ifndef RELEASE
    double elapsed = difftime(time(NULL), client->connected);
    debug("[+] client: stayed %.f seconds, %lu commands\n", elapsed, client->commands);
    #endif

    // closing socket
    close(client->fd);

    // cleaning client memory usage
    free(client);

    // allow new client on this spot
    clients.list[fd] = NULL;

    // maybe we could reduce the list usage now
}

int redis_listen(char *listenaddr, int port) {
    struct sockaddr_in addr_listen;
    redis_handler_t redis;

    // allocating space for clients
    clients.length = REDIS_CLIENTS_INITIAL_LENGTH;

    if(!(clients.list = calloc(sizeof(redis_client_t *), clients.length)))
        diep("clients malloc");

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
    int handler = socket_handler(&redis);

    // cleaning clients list
    for(size_t i = 0; i < clients.length; i++)
        if(clients.list[i])
            socket_client_free(i);

    free(clients.list);

    // notifing source that we are done
    return handler;
}
