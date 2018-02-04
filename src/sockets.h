#ifndef ZDB_SOCKETS_H
    #define ZDB_SOCKETS_H

    #ifdef __linux__
        #include "socket_epoll.h"
    #else
        #error "Plateform not supported"
    #endif
#endif
