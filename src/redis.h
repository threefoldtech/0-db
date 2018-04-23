#ifndef __ZDB_REDIS_H
    #define __ZDB_REDIS_H

    // redis_hardsend is a macro which allows us to send
    // easily a hardcoded message to the client, without needing to
    // specify the size, but keeping the size computed at compile time
    //
    // this is useful when you want to write message to client without
    // writing yourself the size of the string. please only use this with strings
    //
    // sizeof(message) will contains the null character, to append \r\n the size will
    // just be +1
    #define redis_hardsend(fd, message) redis_reply_stack(fd, message "\r\n", sizeof(message) + 1)

    //
    // redis protocol oriented objects
    //
    typedef enum resp_type_t {
        INTEGER,
        STRING,
        NIL,

    } resp_type_t;

    typedef struct resp_object_t {
        resp_type_t type;
        void *buffer;
        int length;
        int filled;

    } resp_object_t;

    typedef enum resp_state_t {
        RESP_EMPTY,
        RESP_FILLIN_HEADER,
        RESP_FILLIN_PAYLOAD,

    } resp_state_t;

    // represent one redis command with arguments
    typedef struct resp_request_t {
        resp_state_t state;
        int fillin;
        int argc;
        resp_object_t **argv;

    } resp_request_t;

    typedef enum resp_status_t {
        RESP_STATUS_SUCCESS,
        RESP_STATUS_ABNORMAL,
        RESP_STATUS_DISCARD,
        RESP_STATUS_DISCONNECTED,
        RESP_STATUS_CONTINUE,
        RESP_STATUS_DONE,
        RESP_STATUS_SHUTDOWN,

    } resp_status_t;

    // tracking each clients in memory
    // we need to create object per client to keep
    // some session-life persistant data, such namespace
    // attached to the socket, and so on

    typedef struct buffer_t {
        char *buffer;
        size_t length;
        size_t remain;
        char *reader;
        char *writer;

    } buffer_t;

    typedef struct redis_response_t {
        void *buffer;  // begin of the buffer which will be free's
        void *reader;  // current pointer on the buffer, for the next chunk to send
        size_t length; // length of the remain payload to send

        // pointer to a desctuctor function which will be
        // called when the send if fully complete, to clean
        // the buffer
        void (*destructor)(void *target);

    } redis_response_t;

    // represent one client in memory
    typedef struct redis_client_t {
        int fd;           // socket file descriptor
        namespace_t *ns;  // connection namespace
        time_t connected; // connection time
        size_t commands;  // request (commands) counter
        int writable;     // does the client can write on the namespace
        int admin;        // does the client is admin
        buffer_t buffer;  // per-client buffer

        // each client will be attached to a request
        // this request will contains one-per-one commands
        resp_request_t *request;

        // each client will have some (optional) pending
        // write, we attach a delayed async writer per
        // client
        redis_response_t response;

    } redis_client_t;

    // represent all clients in memory
    typedef struct redis_clients_t {
        size_t length;
        redis_client_t **list;

    } redis_clients_t;

    // minimum (default) amount of client pre-allocated
    #define REDIS_CLIENTS_INITIAL_LENGTH 32

    // per client buffer
    #define REDIS_BUFFER_SIZE 8192

    // maximum payload size
    #define REDIS_MAX_PAYLOAD 8 * 1024 * 1024

    typedef struct redis_handler_t {
        int mainfd;  // main socket handler
        int evfd;    // event handler (epoll, kqueue, ...)

    } redis_handler_t;

    typedef struct redis_bulk_t {
        size_t length;
        size_t writer;
        unsigned char *buffer;

    } redis_bulk_t;

    int redis_listen(char *listenaddr, int port, char *socket);
    resp_status_t redis_chunk_read(int fd);
    resp_status_t redis_delayed_write(int fd);

    void socket_nonblock(int fd);
    void socket_block(int fd);

    void redis_bulk_append(redis_bulk_t *bulk, void *data, size_t length);
    redis_bulk_t redis_bulk(void *payload, size_t length);

    // abstract handler implemented by a plateform dependent
    // code (see socket_epoll, socket_kqueue, ...)
    int socket_handler(redis_handler_t *handler);

    // managing clients
    redis_client_t *socket_client_new(int fd);
    void socket_client_free(int fd);
    int redis_detach_clients(namespace_t *namespace);

    // socket generic reply
    int redis_reply(redis_client_t *client, void *payload, size_t length, void (*destructor)(void *target));
    int redis_reply_stack(redis_client_t *client, void *payload, size_t length);
#endif
