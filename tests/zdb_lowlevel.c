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
        // redisReconnect(test->zdb);
        initialize_tcp(test);
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

// testing tricky case when the payload is just
// enough to make the final \r\n between two buffer
// read server side (by default buffer is 8192 bytes)
runtest_prio(sp, lowlevel_tricky_buffer_limit) {
    if(test->mode == SEQUENTIAL)
        return TEST_SKIPPED;

    char buffer[8195];
    ssize_t length;

    memcpy(buffer, "*3\r\n$3\r\nSET\r\n$3\r\nXXX\r\n$8164\r\n", 29);
    memset(buffer + 29, 'K', 8193 - 29);
    memcpy(buffer + 8193, "\r\n", 2);

    if(write(test->zdb->fd, buffer, sizeof(buffer)) < 0)
        perror("write");

    if((length = read(test->zdb->fd, buffer, sizeof(buffer))) < 0)
        perror("read");

    if(memcmp(buffer, "$3\r\nXXX\r\n", 9) == 0)
        return TEST_SUCCESS;

    return TEST_FAILED;
}

// testing tricky case when the header of a field is just
// between two buffer read server side
// (by default buffer is 8192 bytes)
runtest_prio(sp, lowlevel_tricky_buffer_header_limit) {
    if(test->mode == SEQUENTIAL)
        return TEST_SKIPPED;

    char buffer[8201];
    ssize_t length;

    memcpy(buffer, "*2\r\n$8178\r\n", 11);
    memset(buffer + 11, 'W', 8178);
    memcpy(buffer + 8189, "\r\n$4\r\nXXXX\r\n", 12);

    if(write(test->zdb->fd, buffer, sizeof(buffer)) < 0)
        perror("write");

    if((length = read(test->zdb->fd, buffer, sizeof(buffer))) < 0)
        perror("read");

    if(memcmp(buffer, "-Command not supported", 22) == 0)
        return TEST_SUCCESS;

    return TEST_FAILED;
}

// testing tricky case when the header of a field is just
// at the end of one packet buffer
// (by default buffer is 8192 bytes)
runtest_prio(sp, lowlevel_tricky_buffer_header_split) {
    if(test->mode == SEQUENTIAL)
        return TEST_SKIPPED;

    char buffer[8198];
    ssize_t length;

    memcpy(buffer, "*2\r\n$8175\r\n", 11);
    memset(buffer + 11, 'W', 8175);
    memcpy(buffer + 8186, "\r\n$4\r\nXXXX\r\n", 12);

    if(write(test->zdb->fd, buffer, sizeof(buffer)) < 0)
        perror("write");

    if((length = read(test->zdb->fd, buffer, sizeof(buffer))) < 0)
        perror("read");

    if(memcmp(buffer, "-Command not supported", 22) == 0)
        return TEST_SUCCESS;

    return TEST_FAILED;
}




#define MAX_CONNECTIONS  128
runtest_prio(sp, lowlevel_open_many_connection) {
    redisContext *ctx[MAX_CONNECTIONS] = {NULL};
    int response = TEST_SUCCESS;

    if(test->type != CONNECTION_TYPE_TCP)
        return TEST_SKIPPED;

    for(unsigned int i = 0; i < MAX_CONNECTIONS; i++) {
        ctx[i] = redisConnect(test->host, test->port);
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


