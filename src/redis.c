#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <limits.h>
#include "zerodb.h"
#include "index.h"
#include "data.h"

#define MAXEVENTS 64

typedef enum resp_type_t {
    INTEGER,
    STRING,
    NIL,

} resp_type_t;

typedef struct resp_object_t {
    resp_type_t type;
    void *buffer;
    int length;

} resp_object_t;

typedef struct resp_request_t {
    int fd;
    int argc;
    resp_object_t **argv;

} resp_request_t;

typedef struct redis_handler_t {
    int mainfd;
    int epollfd;

} redis_handler_t;

static int yes = 1;

// main worker when a redis command was successfuly parsed
static int dispatcher(resp_request_t *request) {
    if(request->argv[0]->type != STRING) {
        debug("[+] not a string command, ignoring\n");
        return 0;
    }

    // PING
    if(!strncmp(request->argv[0]->buffer, "PING", request->argv[0]->length)) {
        verbose("[+] redis: PING\n");
        send(request->fd, "+PONG\r\n", 7, 0);
        return 0;
    }

    // SET
    if(!strncmp(request->argv[0]->buffer, "SET", request->argv[0]->length)) {
        if(request->argc != 3) {
            send(request->fd, "-Invalid argument\r\n", 19, 0);
            return 1;
        }

        if(request->argv[1]->length > 255) {
            send(request->fd, "-Key too large\r\n", 16, 0);
            return 1;
        }

        // create some easier accessor
        unsigned char *id = request->argv[1]->buffer;
        uint8_t idlength = request->argv[1]->length;

        unsigned char *value = request->argv[2]->buffer;
        uint32_t valuelength = request->argv[2]->length;

        // printf("[+] set command: %u bytes key, %u bytes data\n", idlength, valuelength);
        // printf("[+] set key: %.*s\n", idlength, id);
        // printf("[+] set value: %.*s\n", request->argv[2]->length, (char *) request->argv[2]->buffer);

        // insert the data on the datafile
        // this will returns us the offset where the header is
        size_t offset = data_insert(value, valuelength, id, idlength);

        // inserting this offset with the id on the index
        index_entry_insert(id, idlength, offset, request->argv[2]->length);

        // building response
        // here, from original redis protocol, we don't reply with a basic
        // OK or Error when inserting a key, we reply with the key itself
        //
        // this is how the sequential-id can returns the id generated
        char response[MAX_KEY_LENGTH + 8];
        sprintf(response, "+%.*s\r\n", idlength, id);
        send(request->fd, response, strlen(response), 0);

        // checking if we need to jump to the next files
        // we do this check here and not from data (event if this is like a
        // datafile event) to keep data and index code completly distinct
        if(offset + request->argv[2]->length > 100 * 1024 * 1024) { // 100 MB
            size_t newid = index_jump_next();
            data_jump_next(newid);
        }

        return 0;
    }

    // GET
    if(!strncmp(request->argv[0]->buffer, "GET", request->argv[0]->length)) {
        if(request->argv[1]->length > MAX_KEY_LENGTH) {
            printf("[-] invalid key size\n");
            send(request->fd, "-Invalid key\r\n", 14, 0);
            return 1;
        }

        // printf("[+] requesting key: %.*s\n", request->argv[1]->length, (char *) request->argv[1]->buffer);
        index_entry_t *entry = index_entry_get(request->argv[1]->buffer, request->argv[1]->length);

        // checking if ket exists and if it was not deleted
        if(!entry || entry->flags & INDEX_ENTRY_DELETED) {
            verbose("[-] key not found or deleted\n");
            send(request->fd, "$-1\r\n", 5, 0);
            return 1;
        }

        // convert data length integer to string
        char strsize[64];
        size_t stroffset = sprintf(strsize, "%lu", entry->length);

        // $(xx)\r\n + (payload) + \r\n
        size_t total = 1 + stroffset + 2 + entry->length + 2;
        unsigned char *payload = data_get(entry->offset, entry->length, entry->dataid, entry->idlength);

        if(!payload) {
            printf("[-] cannot read payload\n");
            send(request->fd, "-Internal Error\r\n", 17, 0);
            free(payload);
            return 0;
        }

        char *response = malloc(total);

        // build redis response
        strcpy(response, "$");
        strcat(response, strsize);
        strcat(response, "\r\n");

        // appending to response (stroffset + 3 for skipping already written)
        memcpy(response + stroffset + 3, payload, entry->length);
        memcpy(response + total - 2, "\r\n", 2);

        send(request->fd, response, total, 0);

        free(response);
        free(payload);

        return 0;
    }

    if(!strncmp(request->argv[0]->buffer, "DEL", request->argv[0]->length)) {
        if(request->argv[1]->length > MAX_KEY_LENGTH) {
            printf("[-] invalid key size\n");
            send(request->fd, "-Invalid key\r\n", 14, 0);
            return 1;
        }

        index_entry_delete(request->argv[1]->buffer, request->argv[1]->length);
        send(request->fd, "+OK\r\n", 5, 0);

        return 0;
    }


    // STOP (should only be used as debug, to check memory leaks or so)
    if(!strncmp(request->argv[0]->buffer, "STOP", request->argv[0]->length)) {
        send(request->fd, "+Stopping\r\n", 11, 0);
        return 2;
    }

    // unknown
    printf("[-] unsupported redis command\n");
    send(request->fd, "-Command not supported\r\n", 24, 0);

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
static int resp(int fd) {
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

        dvalue = dispatcher(&command);

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

static void socket_nonblock(int fd) {
    int flags;

    if((flags = fcntl(fd, F_GETFL, 0)) < 0)
        diep("fcntl");

    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void socket_block(int fd) {
    int flags;

    if((flags = fcntl(fd, F_GETFL, 0)) < 0)
        diep("fcntl");

    fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
}


static int socket_event(struct epoll_event *events, int notified, redis_handler_t *redis) {
    struct epoll_event *ev;

    for(int i = 0; i < notified; i++) {
        ev = events + i;

        // epoll issue
        // discarding this client
        if((ev->events & EPOLLERR) || (ev->events & EPOLLHUP) || (!(ev->events & EPOLLIN))) {
            warnp("epoll");
            close(ev->data.fd);

            continue;
        }

        // main socket event: we have a new client
        // creating the new client and accepting it
        if(ev->data.fd == redis->mainfd) {
            int clientfd;
            char *client_ip;
            struct sockaddr_in addr_client;
            socklen_t addr_client_len;

            addr_client_len = sizeof(addr_client);

            if((clientfd = accept(redis->mainfd, (struct sockaddr *)&addr_client, &addr_client_len)) == -1)
                warnp("accept");

            socket_nonblock(clientfd);

            client_ip = inet_ntoa(addr_client.sin_addr);
            verbose("[+] incoming connection from %s\n", client_ip);

            // adding client to the epoll list
            struct epoll_event event;

            memset(&event, 0, sizeof(struct epoll_event));

            event.data.fd = clientfd;
            event.events = EPOLLIN;

            if(epoll_ctl(redis->epollfd, EPOLL_CTL_ADD, clientfd, &event) < 0)
                warnp("epoll_ctl");

            continue;
        }

        // here is the "blocking" state
        // which only allows us to parse one client at a time
        // we will only proceed a full request at a time
        //
        // -- FIXME --
        // basicly here is one security issue, a client could
        // potentially lock the daemon by starting a command and not complete it
        //
        socket_block(ev->data.fd);

        // dispatching client event
        int ctrl = resp(ev->data.fd);

        // client error, we discard it
        if(ctrl == 3) {
            close(ev->data.fd);
            continue;
        }

        socket_nonblock(ev->data.fd);

        // dirty way the STOP event is handled
        if(ctrl == 2) {
            printf("[+] stopping daemon\n");
            close(redis->mainfd);
            return 1;
        }
    }

    return 0;
}

static int socket_handler(redis_handler_t *handler) {
    struct epoll_event event;
    struct epoll_event *events = NULL;

    // initialize empty struct
    memset(&event, 0, sizeof(struct epoll_event));

    if((handler->epollfd = epoll_create1(0)) < 0)
        diep("epoll_create1");

    event.data.fd = handler->mainfd;
    event.events = EPOLLIN;

    if(epoll_ctl(handler->epollfd, EPOLL_CTL_ADD, handler->mainfd, &event) < 0)
        diep("epoll_ctl");

    events = calloc(MAXEVENTS, sizeof event);

    // waiting for clients
    // this is how we supports multi-client using a single thread
    // note that, we will only proceed one request at a time
    // allows multiple client to be connected

    while(1) {
        int n = epoll_wait(handler->epollfd, events, MAXEVENTS, -1);
        if(socket_event(events, n, handler) == 1) {
            free(events);
            return 1;
        }
    }

    return 0;
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

    printf("[+] listening on %s:%d\n", listenaddr, port);

    // entering the worker loop
    return socket_handler(&redis);
}
