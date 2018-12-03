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
// when we need to send a response to a client, we try to do it without
// any extra allocation, we just send data as it, if they was sent in one shot, there
// is nothing more to do, otherwise we need to start queuing stuff
//
// if a queue for a client exists, we can't do our preliminary send anymore, since this
// could break the protocol stream
//
// the response object have a buffer, a reader pointer and a destruction function pointer
// which is used to destroy the buffer when it's not needed anymore
redis_response_t *redis_response_new(void *payload, size_t length, void (*destructor)(void *)) {
    redis_response_t *response;

    if(!(response = calloc(sizeof(redis_response_t), 1)))
        return NULL;

    response->buffer = payload;
    response->length = length;
    response->reader = response->buffer;
    response->destructor = destructor;

    return response;
}

// clean the response object and call the destructor
// if set, to clean the buffer
void redis_response_free(redis_response_t *response) {
    if(response->destructor)
        response->destructor(response->buffer);

    free(response);
}

// add a response to the client responses queue
void redis_response_push(redis_client_t *client, redis_response_t *response) {
    // no pending response was there, just point to the new one
    if(client->responses == NULL) {
        client->responses = response;
        client->responsetail = response;
        response->next = NULL;
        return;
    }

    // there are already pending response on the queue
    // appending our response to the list
    client->responsetail->next = response;
    client->responsetail = response;
}

// try to send a response to a client, if succeed returns NULL
// otherwise update reader on the response and returns it (there are more stuff
// to do, but later, now client is busy)
redis_response_t *redis_send_response(redis_client_t *client, redis_response_t *response) {
    ssize_t sent;

    while(response->length > 0) {
        debug("[+] redis: sending reply to %d (%ld bytes remains)\n", client->fd, response->length);

        if((sent = send(client->fd, response->reader, response->length, 0)) < 0) {
            if(errno != EAGAIN) {
                warnp("redis_send_reply: send");

                // we reply NULL because this is an error, the socket is not
                // ready to receive this anyway, we won't sent it at all
                // we won't tell the caller to push this on a queue
                return NULL;
            }

            debug("[-] redis: send: client %d is not ready for the send\n", client->fd);

            // we still have some data to send, which could not
            // be sent because the socket is not ready, and we are in
            // non-blocking mode
            //
            // we don't change anything to the buffer
            // and we wait the next trigger from the polling system
            // to ask us the write is available again
            return response;
        }

        response->reader += sent;
        response->length -= sent;
    }

    debug("[+] redis: send: buffer sucessfully sent\n");
    return NULL;
}

// callback called when a socket becomes available in write
// this mean the client was waiting something (in theory), so let's
// start sending the buffer/queue attached to that client
resp_status_t redis_delayed_write(int fd) {
    redis_client_t *client = clients.list[fd];
    redis_response_t *response;

    if(!client || client->responses == NULL) {
        debug("[+] redis: nothing to send to client (fd: %d)\n", fd);
        return 0;
    }

    response = client->responses;

    debug("[+] redis: sending available buffer to socket %d\n", fd);
    while(response) {
        // sending this response
        // if the send_response returns us something, then it
        // was not fully sent, let's try again later, we are done for now
        if(redis_send_response(client, response) != NULL)
            return 0;

        // this response was successfuly sent
        // let's remove it from the list and keep going

        // saving the next pointer
        redis_response_t *next = response->next;

        // freeing this response
        redis_response_free(response);
        response = next;

        // updating list
        client->responses = next;

        // this was the last response, cleaning the tail
        if(next == NULL)
            client->responsetail = NULL;
    }

    return 0;
}

// entry point when you want to send data to the client, and the buffer
// was allocated on the heap (malloc), this function will just take the payload
// create a response based on that, and send it (pushing on the queue if needed)
int redis_reply_heap(redis_client_t *client, void *payload, size_t length, void (*destructor)(void *)) {
    redis_response_t *response;

    // create a response based on parameters
    if(!(response = redis_response_new(payload, length, destructor))) {
        warnp("redis_reply_head: malloc");
        return 1;
    }

    if(client->responses == NULL) {
        // try to send this response a first time
        if(redis_send_response(client, response) == NULL) {
            pdebug("[+] redis: reply heap: send was made in single shot\n");
            redis_response_free(response);
            return 0;
        }
    }

    // we could not send that response this time, for some reason
    // pushing this response to the client queue
    redis_response_push(client, response);

    return 0;
}

// entry point when you want to send data to the client and the buffer
// is stack allocated (hardcoded string, stack buffer, anything which can't be free'd
// and can't be reached anymore when call is done)
//
// we first try to send it as it, and if this succeed, we're done, otherwise
// we duplicate that data and push it to the client queue
int redis_reply_stack(redis_client_t *client, void *payload, size_t length) {
    redis_response_t response;

    response.buffer = payload;
    response.reader = payload;
    response.length = length;
    response.destructor = NULL;

    // try to send this response a first time, without any extra allocation
    // usually from the stack this will be enough
    //
    // this can only be done if nothing was pending, otherwise we will
    // break protocol serialization (some pending stuff needs to be sent before)
    if(client->responses == NULL) {
        if(redis_send_response(client, &response) == NULL) {
            pdebug("[+] redis: reply stack: no stack duplication needed\n");
            return 0;
        }
    }

    // we could not send that response this time, for some reason
    // we need now to duplicate response, to keep it somewhere on the heap
    // and push it to the sending queue of that client
    redis_response_t *newresponse;
    void *copypayload;

    // duplicate payload
    if(!(copypayload = malloc(length)))
        return 1;

    memcpy(copypayload, payload, length);

    if(!(newresponse = redis_response_new(copypayload, length, free)))
        return 1;

    // maybe some part of the buffer was already sent
    // but not everything, this means we need to reflect that change
    // on the duplicated buffer we just made
    if(response.reader != response.buffer) {
        size_t difference = response.reader - response.buffer;
        newresponse->reader += difference;
        newresponse->length -= difference;
    }

    // pushing this response to the client queue
    redis_response_push(client, newresponse);

    return 0;
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

// check the owner id of the request
// if the request was made by ourself and doesn't come from us
// (come from a replication), returns 1 and ask parent to not proceed
// the request
static inline int redis_handle_resp_ownerid(redis_client_t *client) {
    // request doesn't comes from a replication client
    // it's a normal client, there is nothing special to do
    // just set the owner id as our own id
    if(!client->master) {
        client->request->owner = rootsettings.iid;
        return 0;
    }

    // this comes from a replication client
    // extracting the owner id (which is the last argument)
    // and checking if it's a replay of our own database
    resp_object_t *ownobj = client->request->argv[client->request->argc - 1];
    if(ownobj->length > 32) {
        debug("[-] redis: owner check: malformed owner, id is too long, dropping\n");
        return 1;
    }

    // moving buffer into a temporary string
    char temp[34];
    memcpy(temp, ownobj->buffer, ownobj->length);
    temp[ownobj->length] = '\0';

    // converting string into unsigned integer
    uint32_t ownerid = strtoul(temp, NULL, 10);

    if(ownerid == rootsettings.iid) {
        debug("[-] redis: owner check: looks like this comes from us, dropping\n");
        return 1;
    }

    // okay, let's pop this request from original object
    // so this request will looks like an original request
    client->request->argc -= 1;
    free(ownobj->buffer);
    free(ownobj);

    // this is a valid replication, let's proceed it and
    // propagate the ownerid
    client->request->owner = ownerid;
    return 0;
}

static resp_status_t redis_handle_resp_finished(redis_client_t *client) {
    resp_request_t *request = client->request;
    int value = 0;

    // setting the request ownerid
    if(redis_handle_resp_ownerid(client)) {
        debug("[-] redis: ownerid requested to ignore this request\n");
        redis_free_request(request);
        request->state = RESP_EMPTY;

        return 1;
    }

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

    // not using calloc to ensure everything is set to
    // default value by ourself and nothing is forget
    if(!(client = malloc(sizeof(redis_client_t)))) {
        warnp("new client malloc");
        return NULL;
    }

    client->fd = fd;
    client->connected = time(NULL);
    client->commands = 0;
    client->executed = NULL;
    client->watching = NULL;
    client->mirror = 0;
    client->master = 0;

    // allocating a fixed buffer
    client->buffer = buffer_new();
    if(!client->buffer.buffer) {
        free(client);
        return NULL;
    }

    // allocating single request object
    if(!(client->request = (resp_request_t *) malloc(sizeof(resp_request_t)))) {
        warnp("new client request malloc");
        free(client);
        return NULL;
    }

    // no pending responses
    client->responses = NULL;
    client->responsetail = NULL;

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


int redis_mirror_client(redis_client_t *source, redis_client_t *target) {
    char temp[256];
    char *buffer;
    time_t timestamp = time(NULL);

    // the forward query is the same as the input one
    // but with two more fields: the socket id and the namespace in
    // which the user is attached to

    // special owner id is zero, do not forward this
    // this is used for administrative query not made to be
    // replicated
    if(source->request->owner == 0) {
        debug("[-] redis: mirror: null-owner, not forwarding\n");
        return 0;
    }

    // computing the buffer size
    size_t length = 0;
    size_t offset = 0;

    // computing length of the array (original one + the 2 fields we prepend)
    length += sprintf(temp, "*%d\r\n", source->request->argc + 3);

    // socket id
    length += sprintf(temp, ":%ld\r\n", timestamp);

    // namespace
    length += snprintf(temp, sizeof(temp), "$%lu\r\n%s\r\n", strlen(source->ns->name), source->ns->name);

    // instance (owner) id
    length += sprintf(temp, ":%u\r\n", source->request->owner);

    // length contains:
    //  - header prefix (string length of the size with header)
    //  - payload (buffer length)
    //  - final \r\n (length: 2)
    for(int i = 0; i < source->request->argc; i++) {
        length += sprintf(temp, "$%d\r\n", source->request->argv[i]->length);
        length += source->request->argv[i]->length + 2;
    }

    debug("[+] redis: mirroring %lu bytes from <%d> to <%d>\n", length, source->fd, target->fd);

    if(!(buffer = malloc(length)))
        return 1;

    // building buffer
    offset += sprintf(buffer, "*%d\r\n", source->request->argc + 3);
    offset += sprintf(buffer + offset, ":%ld\r\n", timestamp);
    offset += sprintf(buffer + offset, "$%lu\r\n%s\r\n", strlen(source->ns->name), source->ns->name);
    offset += sprintf(buffer + offset, ":%u\r\n", source->request->owner);

    for(int i = 0; i < source->request->argc; i++) {
        offset += sprintf(buffer + offset, "$%d\r\n", source->request->argv[i]->length);

        memcpy(buffer + offset, source->request->argv[i]->buffer, source->request->argv[i]->length);
        offset += source->request->argv[i]->length;

        memcpy(buffer + offset, "\r\n", 2);
        offset += 2;
    }

    // redis_reply_delayed(target, buffer, length);
    redis_reply_heap(target, buffer, length, free);

    return 0;
}

// handler executed after each command executed
// basicly for now, walk over all the clients, if they are
// on the same namespace as the current client, checking if
// some waiting was set, if waiting was set on the same handler
// we just called, we trigger (notify) it and unlock it
int redis_posthandler_client(redis_client_t *client) {
    char response[64];

    // the client didn't executed any
    // valid command, nothing to check
    if(!client->executed)
        return 0;

    for(size_t i = 0; i < clients.length; i++) {
        redis_client_t *checking = clients.list[i];

        // client doesn't exists anymore in the meantime
        // or target is the current client
        if(!checking || checking == client)
            continue;

        if(checking->mirror) {
            redis_mirror_client(client, checking);
            continue;
        }

        // or this client is not waiting on commands
        // or this client is not waiting on the same namespace
        // ignoring
        if(!checking || !checking->watching || checking->ns != client->ns)
            continue;

        // matching on the exact command
        // or the wildcard command
        if(checking->watching == client->executed || checking->watching->handler == command_asterisk) {
            char *matching = client->executed->command;

            #ifndef RELEASE
            char *waiting = checking->watching->command;
            debug("[+] redis: trigger: client %d waits on <%s>, trigger <%s>\n", checking->fd, waiting, matching);
            #endif

            // trigger done, discarding watcher
            checking->watching = NULL;

            // sending notification
            snprintf(response, sizeof(response), "+%s\r\n", matching);
            redis_reply_stack(checking, response, strlen(response));
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
    struct hostent *hent;
    int fd;

    if((hent = gethostbyname(listenaddr)) == NULL)
        diep("gethostbyname");

    memcpy(&addr.sin_addr, hent->h_addr_list[0], hent->h_length);

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

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
