#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include "tests_user.h"
#include "tests.h"
#include "zdb_utils.h"

#define sp 700

int lowlevel_send_invalid(test_t *test, char *buffer, size_t buflen) {
    if(write(test->zdb->fd, buffer, strlen(buffer)) != (ssize_t) strlen(buffer)) {
        perror("write");
        return TEST_FAILED_FATAL;
    }

    if(read(test->zdb->fd, buffer, buflen) < 0) {
        perror("read");
        return TEST_FAILED_FATAL;
    }

    if(buffer[0] == '-') {
        // connection was closed, reopen it
        initialize_tcp();
        return TEST_SUCCESS;
    }

    return TEST_FAILED;
}

runtest_prio(sp, lowlevel_slow_ping) {
    char buffer[512];

    if(test->type != CONNECTION_TYPE_TCP)
        return TEST_SKIPPED;

    strcpy(buffer, "*1\r\n$4\r\nPING\r\n");

    for(unsigned int i = 0; i < strlen(buffer); i++) {
        if(write(test->zdb->fd, buffer + i, 1) != 1)
            perror("write");

        usleep(10000);
    }

    if(recv(test->zdb->fd, buffer, sizeof(buffer), MSG_NOSIGNAL) < 0)
        perror("read");

    if(buffer[0] == '+')
        return TEST_SUCCESS;

    return TEST_FAILED;
}

runtest_prio(sp, lowlevel_not_an_array) {
    char buffer[512];

    if(test->type != CONNECTION_TYPE_TCP)
        return TEST_SKIPPED;

    // invalid request, not an array
    strcpy(buffer, "+1\r\n$4\r\nPING\r\n");

    return lowlevel_send_invalid(test, buffer, sizeof(buffer));
}

runtest_prio(sp, lowlevel_no_argc) {
    char buffer[512];

    if(test->type != CONNECTION_TYPE_TCP)
        return TEST_SKIPPED;

    // empty array
    strcpy(buffer, "*0\r\n");

    return lowlevel_send_invalid(test, buffer, sizeof(buffer));
}

runtest_prio(sp, lowlevel_too_many_arguments) {
    char buffer[512];

    if(test->type != CONNECTION_TYPE_TCP)
        return TEST_SKIPPED;

    // too many argument
    strcpy(buffer, "*32\r\n");

    return lowlevel_send_invalid(test, buffer, sizeof(buffer));
}

runtest_prio(sp, lowlevel_not_string_argument) {
    char buffer[512];

    if(test->type != CONNECTION_TYPE_TCP)
        return TEST_SKIPPED;

    // sending array as first argument (neasted array) which is
    // not supported
    strcpy(buffer, "*1\r\n*1\r\n$1\r\nX\r\n");

    return lowlevel_send_invalid(test, buffer, sizeof(buffer));
}

#define MAX_CONNECTIONS  128
runtest_prio(sp, lowlevel_open_many_connection) {
    redisContext *ctx[MAX_CONNECTIONS] = {NULL};
    int response = TEST_SUCCESS;

    if(test->type != CONNECTION_TYPE_TCP)
        return TEST_SKIPPED;

    for(unsigned int i = 0; i < MAX_CONNECTIONS; i++) {
        ctx[i] = redisConnect(test->zdb->tcp.host, test->zdb->tcp.port);
        if(!ctx[i] || ctx[i]->err) {
            response = TEST_FAILED;
            break;
        }
    }

    for(unsigned int i = 0; i < MAX_CONNECTIONS; i++) {
        if(ctx[i])
            redisFree(ctx[i]);
    }

    return response;
}


