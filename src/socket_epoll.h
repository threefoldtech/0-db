#ifndef ZDB_SOCKET_EPOLL_H
    #define ZDB_SOCKET_EPOLL_H

    typedef struct redis_handler_t {
        int mainfd;
        int epollfd;

    } redis_handler_t;

    int socket_handler(redis_handler_t *handler);
#endif
