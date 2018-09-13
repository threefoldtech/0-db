#ifndef USERTESTS_H
    #define USERTESTS_H

    #include <hiredis/hiredis.h>

    #define CONNECTION_TYPE_UNIX  0
    #define CONNECTION_TYPE_TCP   1

    typedef enum rmode_t {
        USERKEY,
        SEQUENTIAL,

    } rmode_t;

    typedef struct test_t {
        redisContext *zdb;
        int type;
        char *host;
        int port;
        rmode_t mode;

    } test_t;

#endif
