#include <stdio.h>
#include <string.h>
#include "usertests.h"
#include "tests.h"

int testresult(redisReply *reply, int value) {
    if(reply)
        freeReplyObject(reply);

    return value;
}


runtest_prio(101, simple_ping) {
    redisReply *reply;

    if(!(reply = redisCommand(test->zdb, "PING")))
        return testresult(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->str, "PONG"))
        return testresult(reply, TEST_FAILED);

    return testresult(reply, TEST_SUCCESS);
}


runtest_prio(102, simple_set_hello) {
    redisReply *reply;

    if(!(reply = redisCommand(test->zdb, "SET %s %s", "hello", "world")))
        return testresult(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->str, "hello"))
        return testresult(reply, TEST_FAILED_FATAL);

    return testresult(reply, TEST_SUCCESS);
}

runtest_prio(103, simple_get_hello) {
    redisReply *reply;

    if(!(reply = redisCommand(test->zdb, "GET %s", "hello")))
        return testresult(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->str, "world"))
        return testresult(reply, TEST_FAILED_FATAL);

    return testresult(reply, TEST_SUCCESS);
}


static int overwrite(test_t *test, char *key, char *original, char *newvalue) {
    redisReply *reply;

    // first set
    if(!(reply = redisCommand(test->zdb, "SET %s %s", key, original)))
        return testresult(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->str, key))
        return testresult(reply, TEST_FAILED_FATAL);

    // overwrite
    if(!(reply = redisCommand(test->zdb, "SET %s %s", key, newvalue)))
        return testresult(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->str, key))
        return testresult(reply, TEST_FAILED_FATAL);

    // check value
    if(!(reply = redisCommand(test->zdb, "GET %s", key)))
        return testresult(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->str, newvalue))
        return testresult(reply, TEST_FAILED_FATAL);

    return testresult(reply, TEST_SUCCESS);

}

runtest_prio(104, simple_overwrite_same_length) {
    return overwrite(test, "overwrite_normal", "original", "newvalue");
}

runtest_prio(104, simple_overwrite_shorter) {
    return overwrite(test, "overwrite_shorter", "original", "new");
}

runtest_prio(104, simple_overwrite_longer) {
    return overwrite(test, "overwrite_longer", "original", "newvaluelonger");
}
