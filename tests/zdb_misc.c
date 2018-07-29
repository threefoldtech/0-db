#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include "tests_user.h"
#include "zdb_utils.h"
#include "tests.h"

// sequential priority
#define sp 800

static char *namespace_misc = "default";

// select this new namespace
runtest_prio(sp, misc_select) {
    const char *argv[] = {"SELECT", namespace_misc};
    return zdb_command(test, argvsz(argv), argv);
}


runtest_prio(sp, misc_time) {
    redisReply *reply;

    if(!(reply = redisCommand(test->zdb, "TIME")))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(reply->type != REDIS_REPLY_ARRAY) {
        log("Not an array: %s\n", reply->str);
        return zdb_result(reply, TEST_FAILED_FATAL);
    }

    if(reply->elements != 2) {
        log("Wrong argument response: %lu\n", reply->elements);
        return zdb_result(reply, TEST_FAILED);
    }

    return TEST_SUCCESS;
}

runtest_prio(sp, misc_info) {
    const char *argv[] = {"INFO"};
    return zdb_command(test, argvsz(argv), argv);
}

runtest_prio(sp, misc_wait_missing_args) {
    const char *argv[] = {"WAIT"};
    return zdb_command_error(test, argvsz(argv), argv);
}

runtest_prio(sp, misc_wait_cmd_not_exists) {
    const char *argv[] = {"WAIT", "nonexisting"};
    return zdb_command_error(test, argvsz(argv), argv);
}

static void *misc_wait_send_ping(void *args) {
    usleep(500000);

    const char *argv[] = {"PING"};
    zdb_command(args, argvsz(argv), argv);

    return NULL;
}

runtest_prio(sp, misc_wait_real) {
    // cloning settings and duplicating connection
    test_t newconn = *test;
    initialize(&newconn);

    // creating waiting thread
    pthread_t thread;
    pthread_create(&thread, NULL, misc_wait_send_ping, &newconn);

    // executing wait command
    const char *argv[] = {"WAIT", "PING"};
    int value = zdb_command_error(test, argvsz(argv), argv);

    // get back from normal
    pthread_join(thread, NULL);

    return value;
}


