#ifndef __RKV_REDIS_H
    #define __RKV_REDIS_H

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

    int redis_listen(char *listenaddr, int port);
#endif
