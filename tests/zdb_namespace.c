#include <stdio.h>
#include <string.h>
#include "tests_user.h"
#include "zdb_utils.h"
#include "tests.h"

// sequential priority
#define sp 160

static char *namespace_created = "test_ns_create";
static char *namespace_protected = "test_ns_protected";
static char *namespace_password = "helloworld";

// select not existing namespace
runtest_prio(sp, namespace_select_not_existing) {
    redisReply *reply;

    if(!(reply = redisCommand(test->zdb, "SELECT notfound")))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(reply->type != REDIS_REPLY_ERROR)
        return zdb_result(reply, TEST_FAILED);

    return zdb_result(reply, TEST_SUCCESS);
}

// create a new namespace
runtest_prio(sp, namespace_create) {
    const char *argv[] = {"NSNEW", namespace_created};
    return zdb_command(test, argvsz(argv), argv);
}

// select this new namespace
runtest_prio(sp, namespace_select_created) {
    const char *argv[] = {"SELECT", namespace_created};
    return zdb_command(test, argvsz(argv), argv);
}

// do a set on this namespace
runtest_prio(sp, namespace_simple_set) {
    return zdb_set(test, "hello", "world");
}

// read the value back
runtest_prio(sp, namespace_simple_get) {
    return zdb_check(test, "hello", "world");
}

// write a new value on this namespace
runtest_prio(sp, namespace_special_set) {
    return zdb_set(test, "special-key", "hello");
}

// read this new value to be sure
runtest_prio(sp, namespace_special_get) {
    return zdb_check(test, "special-key", "hello");
}

// move back to default namespace
runtest_prio(sp, namespace_switchback_default) {
    const char *argv[] = {"SELECT", "default"};
    return zdb_command(test, argvsz(argv), argv);
}

// we should not find "special-key" here (another namespace)
runtest_prio(sp, namespace_default_ensure) {
    const char *argv[] = {"GET", "special-key"};
    return zdb_command_error(test, argvsz(argv), argv);
}
