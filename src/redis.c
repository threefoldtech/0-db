#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/un.h>
#include <fcntl.h>
#include <time.h>
#include <inttypes.h>
#include <errno.h>
#include "sockets.h"
#include "zerodb.h"
#include "index.h"
#include "data.h"
#include "namespace.h"
#include "redis.h"
#include "commands.h"
#include "hook.h"

static int yes = 1;

// list of active clients
static redis_clients_t clients = {
    .length = 0,
    .list = NULL,
};

//
// custom buffer
//
static void buffer_reset(buffer_t *buffer) {
    buffer->length = 0;
    buffer->remain = REDIS_BUFFER_SIZE;
    buffer->reader = buffer->buffer;
    buffer->writer = buffer->buffer;
}

static buffer_t buffer_new() {
    buffer_t buffer;

    // initializing empty buffer
    buffer_reset(&buffer);

    // allocating memory
    if(!(buffer.buffer = (char *) malloc(sizeof(char) * REDIS_BUFFER_SIZE))) {
        warnp("new client buffer malloc");
        return buffer;
    }

    // reset pointer to the new buffer
    buffer.reader = buffer.buffer;
    buffer.writer = buffer.buffer;

    return buffer;
}

static void buffer_free(buffer_t *buffer) {
    free(buffer->buffer);
}

//
// redis socket response
//
// we use non-blocking sockets for all clients
//
// since sending response(s) to clients can take more than one send call
// we need a way to deal with these clients without losing performance
// for the other clients connected, while threading or any parallel execution
// is prohibited by design in this project
//
// the workflow is documented in the function redis_reply.
// basically, if we can't send data (because of EAGAIN), we wait for the socket
// to be ready later, using the main poll system
//
static void redis_send_reset(redis_client_t *client) {
    redis_response_t *response = &client->response;

    if(response->destructor)
        response->destructor(response->buffer);

    response->buffer = NULL;
    response->reader = NULL;
    response->length = 0;
}

int redis_send_reply(redis_client_t *client) {
    redis_response_t *response = &client->response;
    ssize_t sent;

    while(response->length > 0) {
        debug("[+] redis: sending reply to %d (%ld bytes remains)\n", client->fd, response->length);

        if((sent = send(client->fd, response->reader, response->length, 0)) < 0) {
            if(errno != EAGAIN) {
                warnp("redis_send_reply, send");
                return errno;
            }

            debug("[-] redis: send: client %d is not ready for the send\n", client->fd);

            // we still have some data to send, which could not
            // be sent because the socket is not ready, and we are in
            // non-blocking mode
            //
            // we don't change anything to the buffer
            // and we wait the next trigger from the polling system
            // to ask us the write is available again
            return errno;
        }

        response->reader += sent;
        response->length -= sent;
    }

    // the buffer was fully sent, let's clean everything
    // that was related to this
    debug("[+] redis: send: buffer sucessfully sent\n");
    redis_send_reset(client);

    return 0;
}

int redis_reply(redis_client_t *client, void *payload, size_t length, void (*destructor)(void *payload)) {
    redis_response_t *response = &client->response;

    // prepare this request, using the arguments provided
    // even if buffers are literal or stack allocated, we will try to send it
    // a first time before choosing what to do
    response->destructor = destructor;
    response->buffer = payload;
    response->reader = payload;
    response->length = length;

    debug("[+] redis: force sending first chunk (client %d)\n", client->fd);
    if(redis_send_reply(client) != EAGAIN) {
        // no EAGAIN and no buffer set, the send request
        // was sucessful, nothing more to do since everything was
        // already cleaned by redis_send_reply
        if(response->buffer == NULL)
            return 0;

        // something went wrong during the send call
        // and it was not a EAGAIN. this is a real error, so
        // let's clean everything and drop this request
        debug("[-] redis: something went wrong while sending, discarding\n");
        redis_send_reset(client);
    }

    // if we are here, we hit a EAGAIN, this could occur for different
    // reasons:
    //  - the buffer to send is too big to be sent in one shot
    //  - the client is not ready to receive data right now
    //
    // since we are on a fully non-blocking socket, we don't want to block
    // the other clients waiting for this client to be ready
    //
    // this function allows buffer allocation on the heap with a specific
    // destructor in its argument, for these calls there is nothing special to
    // do, but this function can also accept a buffer that was stack allocated
    // or even a literal string. in this case, when we will return, these
    // pointers are not safe anymore as we can't reach them.
    //
    // if no destructor was provided, we will duplicate the buffer in order to
    // keep it safe and send it later
    debug("[+] redis: reply: client %d not ready for sending data\n", client->fd);

    // destructor was set, nothing more to do, everything is already set
    if(destructor)
        return 0;

    debug("[+] redis: duplicating data since it was a stack response\n");

    // a destructor was set, we duplicate the buffer to the heap
    if(!(response->buffer = malloc(response->length))) {
        warnp("redis_reply: malloc");
        return 0;
    }

    // we copy from the response and not the argument of these functions,
    // as some data could already have been sent, and redis_reply has
    // updated the response already
    memcpy(response->buffer, response->reader, response->length);

    response->destructor = free;
    response->reader = response->buffer;

    return 0;
}

inline int redis_reply_stack(redis_client_t *client, void *payload, size_t length) {
    return redis_reply(client, payload, length, NULL);
}

//
// auto-bulk builder/responder
//
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

// macro to wrap literal discard error message
#define resp_discard(client, message) resp_discard_real(client, "-" message "\r\n")

static void resp_discard_real(redis_client_t *client, const char *message) {
    debug("[-] redis: resp: error: %s", message); // message needs to have CRLF

    if(redis_reply_stack(client, (void *) message, strlen(message)) < 0)
        fprintf(stderr, "[-] send failed for error message\n");
}

static void redis_free_request(resp_request_t *request) {
    for(int i = 0; i < request->argc; i++) {
        // prematured end and argv was not yet allocated
        if(!request->argv || !request->argv[i])
            continue;

        free(request->argv[i]->buffer);
        free(request->argv[i]);
    }

    free(request->argv);

    // reset request
    request->argc = 0;
    request->argv = NULL;
}

static resp_status_t redis_handle_resp_empty(redis_client_t *client) {
    resp_request_t *request = client->request;
    buffer_t *buffer = &client->buffer;

    // waiting for a complete line
    if(!(buffer->reader = strchr(buffer->buffer, '\n')))
        return RESP_STATUS_CONTINUE;

    // checking for array an request, we only support array
    // since any command is send using array
    if(*buffer->buffer != '*') {
        debug("[-] resp: request is not an array, rejecting\n");
        resp_discard(client, "Malformed request, array expected");
        return RESP_STATUS_DISCARD;
    }

    // reading the amount of arguments
    request->argc = atoi(buffer->buffer + 1);
    debug("[+] redis: resp: %d arguments\n", request->argc);

    if(request->argc == 0) {
        resp_discard(client, "Missing arguments");
        return RESP_STATUS_ABNORMAL;
    }

    if(request->argc > 8) {
        resp_discard(client, "Too many arguments");
        return RESP_STATUS_ABNORMAL;
    }

    if(!(request->argv = (resp_object_t **) calloc(sizeof(resp_type_t *), request->argc))) {
        warnp("request argv malloc");
        resp_discard(client, "Internal memory error");
        return RESP_STATUS_DISCARD;
    }

    request->state = RESP_FILLIN_HEADER;
    request->fillin = 0; // start fillin argument 0
    buffer->reader += 1; // moving after the '\n'

    return RESP_STATUS_SUCCESS;
}

static resp_status_t redis_handle_resp_header(redis_client_t *client) {
    resp_request_t *request = client->request;
    buffer_t *buffer = &client->buffer;
    char *match;

    // this could occure if the reader was set
    // to the next data and this data is not yet available
    if(buffer->reader > buffer->writer) {
        debug("[-] resp: header: trying to read data not received yet\n");
        return RESP_STATUS_ABNORMAL;
    }

    // waiting for a new line
    if(!(match = strchr(buffer->reader, '\n')))
        return RESP_STATUS_ABNORMAL;

    // checking if it's a string request
    if(*buffer->reader != '$') {
        debug("[-] resp: request is not a string, rejecting\n");
        resp_discard(client, "Malformed query string");
        return RESP_STATUS_ABNORMAL;
    }

    if(!(request->argv[request->fillin] = calloc(sizeof(resp_object_t), 1))) {
        warnp("request argc calloc");
        resp_discard(client, "Internal memory error");
        return RESP_STATUS_DISCARD;
    }

    resp_object_t *argument = request->argv[request->fillin];

    // reading the length of the array
    request->argv[request->fillin]->length = atoi(buffer->reader + 1);

    if(argument->length > REDIS_MAX_PAYLOAD) {
        resp_discard(client, "Payload too big");
        return RESP_STATUS_DISCARD;
    }

    if(!(argument->buffer = (unsigned char *) malloc(argument->length))) {
        warnp("argument buffer malloc");
        resp_discard(client, "Internal memory error");
        return RESP_STATUS_DISCARD;
    }

    argument->type = STRING;

    buffer->reader = match + 1; // set reader after the \n
    request->state = RESP_FILLIN_PAYLOAD;

    return RESP_STATUS_CONTINUE;
}

static resp_status_t redis_handle_resp_payload(redis_client_t *client, int fd) {
    resp_request_t *request = client->request;
    buffer_t *buffer = &client->buffer;
    resp_object_t *argument = request->argv[request->fillin];

    ssize_t available = buffer->length - (buffer->reader - buffer->buffer);

    // do we have available data on the buffer
    if(available == 0) {
        debug("[-] resp: reading payload: no data available on the buffer\n");
        return RESP_STATUS_SUCCESS;
    }

    if(available < argument->length - argument->filled && buffer->remain > 1) {
        // no enough data but the buffer is not full, let's wait for
        // more data to come
        return RESP_STATUS_ABNORMAL;
    }

    // we don't have enough data on the buffer
    // to fill the complete payload
    //
    // let's save what we have and call again the whole process.
    // this process will succeed until valid request is received or
    // until an EAGAIN which means we need to wait for more data
    if(available < argument->length - argument->filled) {
        memcpy(argument->buffer + argument->filled, buffer->reader, available);
        argument->filled += available;

        buffer_reset(&client->buffer);
        return redis_chunk_read(fd);
    }

    // computing the amount of data we need to extract
    // from the buffer
    size_t needed = argument->length - argument->filled;

    memcpy(argument->buffer + argument->filled, buffer->reader, needed);
    argument->filled += needed;

    // jumping to the next argument, this one is set
    request->fillin += 1;
    request->state = RESP_FILLIN_HEADER;
    buffer->reader += argument->length + 2; // skipping the last \r\n

    return RESP_STATUS_CONTINUE;
}

static resp_status_t redis_handle_resp_finished(redis_client_t *client) {
    resp_request_t *request = client->request;
    int value = 0;

    debug("[+] redis: request parsed, calling dispatcher\n");
    value = redis_dispatcher(client);
    debug("[+] redis: dispatcher done, return code: %d\n", value);

    // clearing the request
    redis_free_request(request);

    // reset states
    buffer_reset(&client->buffer);
    request->state = RESP_EMPTY;

    return value;
}

// int redis_read_client(int fd) {
resp_status_t redis_chunk_read(int fd) {
    redis_client_t *client = clients.list[fd];
    resp_request_t *request = client->request;
    buffer_t *buffer = &client->buffer;
    ssize_t length;

    if(buffer->remain == 0) {
        debug("[-] resp: new chunk requested and buffer full\n");
        return RESP_STATUS_DISCARD;
    }

    if((length = recv(fd, buffer->writer, buffer->remain - 1, 0)) < 0) {
        if(errno != EAGAIN && errno != EWOULDBLOCK) {
            warnp("client recv");
            return RESP_STATUS_ABNORMAL;
        }

        // we hit a EGAIN or EWOULDBLOCK, nothing wrong here
        return RESP_STATUS_SUCCESS;
    }

    buffer->writer += length;
    buffer->length += length;
    buffer->remain -= length;

    // ensure string (needed for testing later)
    buffer->buffer[buffer->length] = '\0';

    if(length == 0) {
        debug("[+] resp: empty socket read, client disconnected\n");
        return RESP_STATUS_DISCONNECTED;
    }

    int value;

    // the current request is empty
    // let's let's check if we have enough to build
    // the original header
    if(request->state == RESP_EMPTY)
        if((value = redis_handle_resp_empty(client)) != RESP_STATUS_SUCCESS)
            return value;

    while(1) {
        // if this was the last argument, execute the request
        if(request->fillin == request->argc) {
            // we have everything we need to proceed but
            // we still wait for receiving the last \r\n from the client
            if(strncmp(buffer->buffer + buffer->length - 2, "\r\n", 2) != 0)
                return RESP_STATUS_CONTINUE;

            // executing the request
            return redis_handle_resp_finished(client);
        }

        if(request->state == RESP_FILLIN_HEADER) {
            if((value = redis_handle_resp_header(client)) != RESP_STATUS_CONTINUE)
                return value;
        }

        if(request->state == RESP_FILLIN_PAYLOAD) {
            if((value = redis_handle_resp_payload(client, fd)) != RESP_STATUS_CONTINUE)
                return value;
        }
    }

    return 0;
}

resp_status_t redis_delayed_write(int fd) {
    redis_client_t *client = clients.list[fd];

    if(!client || !client->response.buffer)
        return 0;

    debug("[+] redis: write available to socket %d, sending data\n", fd);
    redis_send_reply(client);

    return 0;
}

void socket_nonblock(int fd) {
    int flags;

    if((flags = fcntl(fd, F_GETFL, 0)) < 0)
        diep("fcntl");

    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// allocate a new client for a new file descriptor
// used to keep session-live information about clients
redis_client_t *socket_client_new(int fd) {
    debug("[+] new client (fd: %d)\n", fd);

    if(fd >= (int) clients.length) {
        redis_client_t **newlist = NULL;
        size_t newlength = clients.length + fd;

        // growing the list
        if(!(newlist = (redis_client_t **) realloc(clients.list, sizeof(redis_client_t *) * newlength)))
            return NULL;

        // ensure new clients are not set
        for(size_t i = clients.length; i < newlength; i++)
            newlist[i] = NULL;

        // increase client list
        clients.list = newlist;
        clients.length = newlength;
    }

    redis_client_t *client = NULL;

    if(!(client = malloc(sizeof(redis_client_t)))) {
        warnp("new client malloc");
        return NULL;
    }

    client->fd = fd;
    client->connected = time(NULL);
    client->commands = 0;

    // allocate a fixed buffer
    client->buffer = buffer_new();
    if(!client->buffer.buffer)
        return NULL;

    // allocate a single request object
    if(!(client->request = (resp_request_t *) malloc(sizeof(resp_request_t)))) {
        warnp("new client request malloc");
        return NULL;
    }

    memset(&client->response, 0x00, sizeof(redis_response_t));

    client->request->state = RESP_EMPTY;
    client->request->argc = 0;
    client->request->argv = NULL;

    // attach default namespace to this client
    client->ns = namespace_get_default();

    // by default, the default namespace is writable
    client->writable = 1;

    // set all users to admin if no password are set
    client->admin = (rootsettings.adminpwd) ? 0 : 1;

    // set client to the list
    clients.list[fd] = client;

    return client;
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
    redis_free_request(client->request);
    buffer_free(&client->buffer);

    free(client->request);
    free(client);

    // allow new client on this spot
    clients.list[fd] = NULL;

    // maybe we could reduce the list usage now
}

// walk over all clients and match them by provided namespace
// if they match, movie them to a special state, waiting
// for disconnection (with alert)
int redis_detach_clients(namespace_t *namespace) {
    for(size_t i = 0; i < clients.length; i++) {
        if(!clients.list[i])
            continue;

        if(clients.list[i]->ns == namespace) {
            debug("[+] redis: client %d: waiting for disconnection\n", clients.list[i]->fd);
            clients.list[i]->ns = NULL;
        }
    }

    return 0;
}

// one namespace is removed
// we need to move the client attached to this namespace
// to a non-valid namespace, in order to notify them

// classic tcp socket
static int redis_tcp_listen(char *listenaddr, int port) {
    struct sockaddr_in addr;
    int fd;

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr(listenaddr);

    if((fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
        diep("tcp socket");

    if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1)
        diep("tcp setsockopt");

    if(bind(fd, (struct sockaddr*) &addr, sizeof(addr)) == -1)
        diep("tcp bind");

    return fd;
}

// unix socket
static int redis_unix_listen(char *socketpath) {
    struct sockaddr_un addr;
    int fd;

    if((fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
        diep("unix socket");

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;

    strncpy(addr.sun_path, socketpath, sizeof(addr.sun_path) - 1);
    unlink(socketpath);

    if(bind(fd, (struct sockaddr*) &addr, sizeof(addr)) == -1)
        diep("unix bind");

    return fd;
}

static void daemonize() {
    pid_t pid = fork();

    if(pid == -1)
        diep("fork");

    if(pid != 0) {
        success("[+] system: forking to background (pid: %d)", pid);
        exit(EXIT_SUCCESS);
    }

    if(rootsettings.logfile) {
        if(!(freopen(rootsettings.logfile, "w", stdout)))
            diep(rootsettings.logfile);

        if(!(freopen(rootsettings.logfile, "w", stderr)))
            diep(rootsettings.logfile);

        // reset stdout to line-buffered
        setvbuf(stdout, NULL, _IOLBF, 0);
    }

    verbose("[+] system: working in background now");
}

static void redis_listen_hook() {
    hook_t *hook = hook_new("ready", 1);

    hook_append(hook, rootsettings.zdbid);

    hook_execute(hook);
    hook_free(hook);
}

int redis_listen(char *listenaddr, int port, char *socket) {
    redis_handler_t redis;

    // allocating space for clients
    clients.length = REDIS_CLIENTS_INITIAL_LENGTH;

    if(!(clients.list = calloc(sizeof(redis_client_t *), clients.length)))
        diep("clients malloc");

    if(socket) {
        redis.mainfd = redis_unix_listen(socket);

    } else {
        redis.mainfd = redis_tcp_listen(listenaddr, port);
    }

    socket_nonblock(redis.mainfd);

    if(listen(redis.mainfd, SOMAXCONN) == -1)
        diep("listen");

    success("[+] listening on: %s", rootsettings.zdbid);

    if(rootsettings.background)
        daemonize();

    // notify we are ready
    if(rootsettings.hook)
        redis_listen_hook();

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
