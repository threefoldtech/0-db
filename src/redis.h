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
    #define redis_hardsend(fd, message) send(fd, message "\r\n", sizeof(message) + 1, 0)

    // tracking each clients in memory
    // we need to create object per client to keep
    // some session-life persistant data, such namespace
    // attached to the socket, and so on

    // represent one client in memory
    typedef struct redis_client_t {
        int fd;           // socket file descriptor
        namespace_t *ns;  // connection namespace
        time_t connected; // connection time
        size_t commands;  // request (commands) counter
        int writable;     // does the client can write on the namespace
        int admin;        // does the client is admin

    } redis_client_t;

    // represent all clients in memory
    typedef struct redis_clients_t {
        size_t length;
        redis_client_t **list;

    } redis_clients_t;

    // minimum (default) amount of client pre-allocated
    #define REDIS_CLIENTS_INITIAL_LENGTH 32

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

    } resp_object_t;

    typedef struct resp_request_t {
        redis_client_t *client;
        int argc;
        resp_object_t **argv;

    } resp_request_t;

    typedef struct redis_handler_t {
        int mainfd;  // main socket handler
        int evfd;    // event handler (epoll, kqueue, ...)

    } redis_handler_t;

    typedef struct redis_bulk_t {
        size_t length;
        size_t writer;
        unsigned char *buffer;

    } redis_bulk_t;

    int redis_listen(char *listenaddr, int port);
    int redis_response(int fd);

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
#endif
