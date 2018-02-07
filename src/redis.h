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
        int mainfd;  // main socket handler
        int evfd;    // event handler (epoll, kqueue, ...)

    } redis_handler_t;

    int redis_listen(char *listenaddr, int port);
    int redis_dispatcher(resp_request_t *request);
    int redis_response(int fd);

    void socket_nonblock(int fd);
    void socket_block(int fd);

    // abstract handler implemented by a plateform dependent
    // code (see socket_epoll, socket_kqueue, ...)
    int socket_handler(redis_handler_t *handler);
#endif
