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

// full protocol debug
// this produce full dump of socket payload
// and lot of debug message about protocol parsing

// #define PROTOCOL_DEBUG

// -- internal static protocol debugger --
#ifdef RELEASE
    #undef PROTOCOL_DEBUG
#endif

#ifdef PROTOCOL_DEBUG
    #define pdebug(...) { printf(__VA_ARGS__); }
#else
    #define pdebug(...) ((void)0)
#endif
// -- internal static protocol debugger --

static int yes = 1;

// list of active clients
static redis_clients_t clients = {
    .length = 0,
    .list = NULL,
};

//
// custom buffer
//

// reset buffer to point like empty
static void buffer_reset(buffer_t *buffer) {
    buffer->length = 0;
    buffer->remain = REDIS_BUFFER_SIZE;
    buffer->reader = buffer->buffer;
    buffer->writer = buffer->buffer;
}

// move the remaining data on the buffer
// into the beginning of the buffer
static void buffer_shift(buffer_t *buffer) {
    if(buffer->reader == buffer->buffer) {
        // nothing to shift
        return;
    }

    pdebug("[+] redis: buffer shifting\n");

    buffer->length = buffer->writer - buffer->reader;
    buffer->remain = REDIS_BUFFER_SIZE - buffer->length;
    memmove(buffer->buffer, buffer->reader, buffer->length);

    buffer->reader = buffer->buffer;
    buffer->writer = buffer->reader + buffer->length;
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
// we use non-blocking socket for all clients
//
// since sending response to client can takes more than one send call
// we need a way to deal with theses clients without loosing performance
// for the others client connected, and threading or anything parallele execution
// is prohibed by design in this project
//
// in the function redis_reply, the workflow is documented
// basicly, if we can't send data (because of EAGAIN), we wait the socket
// to be ready later, by the main poll system
//
static void redis_send_reset(redis_client_t *client) {
    redis_response_t *response = &client->response;

    // free memory
    free(response->buffer);

    // reset structure
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
    // which was related to this
    debug("[+] redis: send: buffer sucessfully sent\n");
    redis_send_reset(client);

    return 0;
}

int redis_reply(redis_client_t *client, void *payload, size_t length) {
    redis_response_t *response = &client->response;

    // we need to send data, we maybe still have something on the buffer
    // if the client was doing some pipeline (maybe we still have something to
    // send, in pending, and we received another command in the mean time, because
    // we received them in batch)
    //
    // we need to append this response to the existing response, the current only easy
    // solution is just growing up the buffer and appending the data

    // saving the current buffer, to free it after
    void *backup = response->buffer;

    // reallocating the buffer, with the new expected size
    // this size if the remaining size of the data in the buffer
    // plus the size we want to add
    response->buffer = malloc(response->length + length);

    // let's copy what's still need to be sent on the existing buffer
    memcpy(response->buffer, response->reader, response->length);

    // now copy the new payload we just want to send
    memcpy(response->buffer + response->length, payload, length);

    // updating structure pointer to point to the new buffer
    response->reader = response->buffer;
    response->length += length;

    // cleaning the old buffer not used anymore
    free(backup);

    debug("[+] redis: force sending first chunk (client %d)\n", client->fd);
    if(redis_send_reply(client) != EAGAIN) {
        // no EAGAIN and no buffer set, the send request
        // was sucessful, nothing more to do since everything was
        // already cleaned by redis_send_reply
        if(response->buffer == NULL)
            return 0;

        // something went wrong during the send call
        // and it was not a EAGAIN, this is a real error
        // let's clean everything and drop this request
        debug("[-] redis: something went wrong while sending, discarding\n");
        redis_send_reset(client);
    }

    // if we are here, we hit a EAGAIN, this could occures for different
    // reasons:
    //  - the buffer to send is too big to be sent in one shot
    //  - the client is not ready to receive data right now
    //
    // since we are full non-blocking socket, we don't want to block
    // the others clients waiting this client is ready
    //
    // we already duplicated the data received, whatever it comes from
    // so we don't have anything more to do, just waiting for the socket
    // to be ready to send the next chunks
    debug("[+] redis: reply: client %d not ready for sending data\n", client->fd);

    return 0;
}

// legacy function when destructor was existing
inline int redis_reply_stack(redis_client_t *client, void *payload, size_t length) {
    return redis_reply(client, payload, length);
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
    char *match;

    // checking if we have a new line character on the
    // request, if yes, we can parse this segment
    if(!(match = memchr(buffer->reader, '\n', buffer->writer - buffer->reader))) {
        // the buffer seems full and we don't have
        // anything usable, let's try to shift the buffer
        // and hope next call will be usable
        if(buffer->remain == 0) {
            buffer_shift(buffer);
            return RESP_STATUS_RESET;
        }

        return RESP_STATUS_CONTINUE;
    }

    // should we check for \r\n ?

    // checking for array request, we only support array
    // since any command are send using array
    if(*buffer->reader != '*') {
        debug("[-] resp: request is not an array, rejecting\n");
        resp_discard(client, "Malformed request, array expected");
        return RESP_STATUS_DISCARD;
    }

    // reading the amount of argument
    request->argc = atoi(buffer->reader + 1);
    debug("[+] redis: resp: %d arguments\n", request->argc);

    if(request->argc == 0) {
        resp_discard(client, "Missing arguments");
        return RESP_STATUS_ABNORMAL;
    }

    // we don't have any command
    // with more than like 4 or 5 arguments
    // but let put a higher limit, just in case
    if(request->argc > 8) {
        resp_discard(client, "Too many arguments");
        return RESP_STATUS_ABNORMAL;
    }

    // allocating needed requests per arguments announced
    // (this is basicly why we limit the number or items)
    if(!(request->argv = (resp_object_t **) calloc(sizeof(resp_type_t *), request->argc))) {
        warnp("request argv malloc");
        resp_discard(client, "Internal memory error");
        return RESP_STATUS_DISCARD;
    }

    // next step if reading the first
    // header of the first argument
    request->state = RESP_FILLIN_HEADER;
    request->fillin = 0;
    buffer->reader = match + 1; // moving after the '\n'

    return RESP_STATUS_SUCCESS;
}

static resp_status_t redis_handle_resp_header(redis_client_t *client) {
    resp_request_t *request = client->request;
    buffer_t *buffer = &client->buffer;
    char *match;

    // this could occures if the reader was set
    // to the next data and theses data are not yet available
    if(buffer->reader > buffer->writer) {
        debug("[-] resp: header: trying to read data not received yet\n");
        return RESP_STATUS_ABNORMAL;
    }

    // waiting for a new line
    if(!(match = memchr(buffer->reader, '\n', buffer->writer - buffer->reader))) {
        // the buffer seems full and we don't have
        // anything usable, let's try to shift the buffer
        // and hope next call will be usable
        if(buffer->remain == 0) {
            buffer_shift(buffer);
            return RESP_STATUS_RESET;
        }

        return RESP_STATUS_ABNORMAL;
    }

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
    argument->length = atoi(buffer->reader + 1);
    // real size is the length + 2 (\r\n)
    argument->size = argument->length + 2;

    if(argument->length > REDIS_MAX_PAYLOAD) {
        resp_discard(client, "Payload too big");
        return RESP_STATUS_DISCARD;
    }

    if(!(argument->buffer = (unsigned char *) malloc(argument->size))) {
        warnp("argument buffer malloc");
        resp_discard(client, "Internal memory error");
        return RESP_STATUS_DISCARD;
    }

    argument->type = STRING;

    buffer->reader = match + 1; // set reader after the \n
    request->state = RESP_FILLIN_PAYLOAD;

    return RESP_STATUS_CONTINUE;
}

static resp_status_t redis_handle_resp_payload(redis_client_t *client) {
    resp_request_t *request = client->request;
    buffer_t *buffer = &client->buffer;
    resp_object_t *argument = request->argv[request->fillin];

    size_t available = buffer->writer - buffer->reader;

    // do we have available data on the buffer
    if(available == 0) {
        // the buffer seems full and we don't have
        // anything usable, let's try to shift the buffer
        // and hope next call will be usable
        if(buffer->remain == 0) {
            buffer_shift(buffer);
            return RESP_STATUS_RESET;
        }

        debug("[-] resp: reading payload: no data available on the buffer\n");
        return RESP_STATUS_CONTINUE;
    }

    // we maybe have too much data on the buffer
    // and we don't need all of them on this payload
    //
    // let's take only what we need and let the
    // caller do the next things
    size_t needed = argument->size - argument->filled;

    pdebug("[+] redis: payload: needed: %lu, available: %lu\n", needed, available);

    if(available >= needed) {
        pdebug("[+] redis: more (or equals) available/needed, extracting needed\n");
        void *argbuf = argument->buffer + argument->filled;
        memcpy(argbuf, buffer->reader, needed);

        // let update counters
        argument->filled += needed;
        request->fillin += 1;
        request->state = RESP_FILLIN_HEADER;
        buffer->reader += needed;

        // special case, if buffer contains exactly what's needed
        // let's reset it after
        // this is not *obligatory* (since reader/writer are updated)
        // but this makes more space for next call
        if(available == needed) {
            pdebug("[+] redis: available was exactly what's needed, reset buffer\n");
            buffer_reset(buffer);
        }

        return RESP_STATUS_CONTINUE;
    }

    pdebug("[+] redis: saving %lu bytes into user request\n", available);

    // we don't have enough data on the buffer
    // to fill the complete payload, let's take everything available
    // and place it on the request buffer, reseting source buffer
    // and waiting for more data to come (by the caller)
    memcpy(argument->buffer + argument->filled, buffer->reader, available);
    argument->filled += available;

    pdebug("[+] redis: resetting buffer\n");
    buffer_reset(&client->buffer);

    return RESP_STATUS_CONTINUE;
}

static resp_status_t redis_handle_resp_finished(redis_client_t *client) {
    resp_request_t *request = client->request;
    int value = 0;

    debug("[+] redis: request parsed, calling dispatcher\n");
    value = redis_dispatcher(client);
    debug("[+] redis: dispatcher done, return code: %d\n", value);

    debug("[+] redis: calling posthandler\n");
    redis_posthandler_client(client);

    // clearing the request
    redis_free_request(request);

    // reset states
    // buffer_reset(&client->buffer);
    request->state = RESP_EMPTY;

    return value;
}

// function called as soon as something is available on
// one client socket
resp_status_t redis_chunk_read(int fd) {
    redis_client_t *client = clients.list[fd];
    resp_request_t *request = client->request;
    buffer_t *buffer = &client->buffer;
    ssize_t length;

    // default return value
    int value = RESP_STATUS_SUCCESS;

go_again:
    // buffer is full, this is probably a bug
    if(buffer->remain == 0) {
        debug("[-] resp: new chunk requested and buffer full\n");
        return RESP_STATUS_DISCARD;
    }

    pdebug("[+] redis: perform read on the socket\n");
    if((length = recv(fd, buffer->writer, buffer->remain, 0)) < 0) {
        if(errno != EAGAIN && errno != EWOULDBLOCK) {
            warnp("client recv");
            return RESP_STATUS_ABNORMAL;
        }

        // we hit a EGAIN or EWOULDBLOCK, nothing wrong here,
        // this is probably because the request was done
        // and nothing more is available on the socket, let's
        // return the caller the value we received from the
        // process (or success if nothing was done)
        return value;
    }

    if(length == 0) {
        // socket was empty
        // this is probably a connection reset by peer
        // let's disconnect this client
        debug("[+] resp: empty socket read, client disconnected\n");
        return RESP_STATUS_DISCONNECTED;
    }

    buffer->writer += length;
    buffer->length += length;
    buffer->remain -= length;

    #ifdef PROTOCOL_DEBUG
    fulldump((uint8_t *) buffer->buffer, buffer->length);
    #endif

    // ensure string (needed for testing later)
    // buffer->buffer[buffer->length] = '\0';

    // while we didn't parsed everything available
    // on the buffer
    while(buffer->reader < buffer->writer) {
        pdebug("[+] redis: buffer parsing (r: %p, w: %p)\n", buffer->reader, buffer->writer);

        // checking if the current request is empty
        // if it is, let's doing a parsing to see if enough
        // data are available to build the request and if
        // the data is well formated
        if(request->state == RESP_EMPTY) {
            if((value = redis_handle_resp_empty(client)) != RESP_STATUS_SUCCESS) {
                // it looks like we didn't had enough data
                // or data was not correctly formated (unexpected data)
                // we don't have anything more to do right now
                break;
            }
        }

        // here, since redis_handle_resp_empty was executed at least one time
        // and returned with success, we know the state is at least something
        // usable, we can now check what we need to do

        // if state is RESP_FILLIN_HEADER, we are waiting for data
        // to be used to fill in the (next) argument header (the type and length)
        if(request->state == RESP_FILLIN_HEADER) {
            pdebug("[+] redis: header parser\n");

            if((value = redis_handle_resp_header(client)) != RESP_STATUS_CONTINUE) {
                // we didn't had enough information or data was invalid (malformed request)
                // nothing more to do right now
                break;
            }
        }

        // if state is RESP_FILLIN_PAYLOAD, we are waiting for data
        // to fill the payload of the argument (we know the type and the length now)
        if(request->state == RESP_FILLIN_PAYLOAD) {
            pdebug("[+] redis: payload parser\n");

            if((value = redis_handle_resp_payload(client)) != RESP_STATUS_CONTINUE) {
                // we didn't had enough information or data was invalid (malformed request)
                // nothing more to do right now
                break;
            }

        }

        // the last argument proceed by the payload
        // completed, and now the argument counter match
        // with argc, we know all arguments was parsed correctly
        // we can do real work with this request
        if(request->fillin == request->argc) {
            pdebug("[+] redis: request completed, executing\n");
            value = redis_handle_resp_finished(client);
        }
    }

    // do not keep going on this request/client
    if(value == RESP_STATUS_DISCARD || value == RESP_STATUS_DISCONNECTED) {
        pdebug("[+] redis: discard or disconnected received\n");
        return value;
    }

    // specific end of work
    if(value == RESP_STATUS_DONE || value == RESP_STATUS_SHUTDOWN) {
        pdebug("[+] redis: done or shutdown received\n");
        return value;
    }

    // not suceed, let's try again
    if(value != RESP_STATUS_SUCCESS) {
        pdebug("[+] redis: parsing didn't suceed, trying again\n");
        goto go_again;
    }

    // success
    pdebug("[+] redis: socket parsing succeed\n");
    return RESP_STATUS_SUCCESS;
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

    redis_client_t *client = NULL;

    if(!(client = malloc(sizeof(redis_client_t)))) {
        warnp("new client malloc");
        return NULL;
    }

    client->fd = fd;
    client->connected = time(NULL);
    client->commands = 0;
    client->executed = NULL;
    client->watching = NULL;

    // allocating a fixed buffer
    client->buffer = buffer_new();
    if(!client->buffer.buffer)
        return NULL;

    // allocating single request object
    if(!(client->request = (resp_request_t *) malloc(sizeof(resp_request_t)))) {
        warnp("new client request malloc");
        return NULL;
    }

    memset(&client->response, 0x00, sizeof(redis_response_t));

    client->request->state = RESP_EMPTY;
    client->request->argc = 0;
    client->request->argv = NULL;

    // attaching default namespace to this client
    client->ns = namespace_get_default();

    // by default, the default namespace is writable
    // except if protect mode is enabled
    client->writable = (rootsettings.protect) ? 0 : 1;

    // set all users admin if no password are set
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
// if they matches, moving them to special state awaiting
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

// handler executed after each command executed
// basicly for now, walk over all the clients, if they are
// on the same namespace as the current client, checking if
// some waiting was set, if waiting was set on the same handler
// we just called, we trigger (notify) it and unlock it
int redis_posthandler_client(redis_client_t *client) {
    for(size_t i = 0; i < clients.length; i++) {
        redis_client_t *checking = clients.list[i];

        if(!checking || checking->ns != client->ns)
            continue;

        if(!checking->watching)
            continue;

        if(checking->watching == client->executed) {
            // trigger done, discarding watcher
            checking->watching = NULL;

            // sending notification
            debug("[+] redis: posthandler: client %d was waiting, notifing\n", checking->fd);
            redis_hardsend(checking, "+OK");
        }
    }

    return 0;
}

// one namespace is removed
// we need to move client attached to this namespace
// to a none-valid namespace, in order to notify them

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

    verbose("[+] system: working on background now");
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
