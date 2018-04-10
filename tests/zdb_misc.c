#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
