#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "tests_user.h"
#include "tests.h"
#include "zdb_utils.h"

runtest_prio(101, simple_ping) {
    redisReply *reply;

    if(!(reply = redisCommand(test->zdb, "PING")))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->str, "PONG"))
        return zdb_result(reply, TEST_FAILED);

    return zdb_result(reply, TEST_SUCCESS);
}

runtest_prio(102, ensure_db_empty) {
    const char *argv[] = {"DBSIZE"};
    long long value = zdb_command_integer(test, argvsz(argv), argv);

    if(value > 0) {
        log("Database not empty !\n");
        log("Theses tests expect empty database.\n");
        log("Some unexpected or false-positive issue could occures.\n");
        log("\n");
        log("You've been warned.\n");
        log("(test will go on in 4 secondes)\n");
        usleep(4000000);

        return TEST_WARNING;
    }

    return TEST_SUCCESS;
}


runtest_prio(110, default_set_hello) {
    return zdb_set(test, "hello", "world");
}

runtest_prio(110, default_get_hello) {
    return zdb_check(test, "hello", "world");
}

runtest_prio(110, default_del_hello) {
    const char *argv[] = {"DEL", "hello"};
    return zdb_command(test, argvsz(argv), argv);
}

runtest_prio(110, default_set_hello_again) {
    return zdb_set(test, "hello", "world-new");
}

runtest_prio(110, default_get_hello_new) {
    return zdb_check(test, "hello", "world-new");
}

// keep a kep deleted
runtest_prio(110, default_set_deleted) {
    return zdb_set(test, "deleted", "yep");
}

runtest_prio(110, default_del_deleted) {
    const char *argv[] = {"DEL", "deleted"};
    return zdb_command(test, argvsz(argv), argv);
}

runtest_prio(110, default_get_deleted) {
    const char *argv[] = {"GET", "deleted"};
    return zdb_command_error(test, argvsz(argv), argv);
}



static int overwrite(test_t *test, char *key, char *original, char *newvalue) {
    redisReply *reply;

    // first set
    if(!(reply = redisCommand(test->zdb, "SET %s %s", key, original)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->str, key))
        return zdb_result(reply, TEST_FAILED_FATAL);

    // overwrite
    if(!(reply = redisCommand(test->zdb, "SET %s %s", key, newvalue)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->str, key))
        return zdb_result(reply, TEST_FAILED_FATAL);

    // check value
    if(!(reply = redisCommand(test->zdb, "GET %s", key)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->str, newvalue))
        return zdb_result(reply, TEST_FAILED_FATAL);

    return zdb_result(reply, TEST_SUCCESS);

}

runtest_prio(115, simple_overwrite_same_length) {
    return overwrite(test, "overwrite_normal", "original", "newvalue");
}

runtest_prio(115, simple_overwrite_shorter) {
    return overwrite(test, "overwrite_shorter", "original", "new");
}

runtest_prio(115, simple_overwrite_longer) {
    return overwrite(test, "overwrite_longer", "original", "newvaluelonger");
}


// command: exists
runtest_prio(120, default_exists_missing_args) {
    const char *argv[] = {"EXISTS"};
    return zdb_command_error(test, argvsz(argv), argv);
}

runtest_prio(120, default_exists_notfound) {
    const char *argv[] = {"EXISTS", "unknown-key"};
    long long value = zdb_command_integer(test, argvsz(argv), argv);

    if(value == 0)
        return TEST_SUCCESS;

    return TEST_FAILED_FATAL;
}

runtest_prio(120, default_exists_deleted) {
    const char *argv[] = {"EXISTS", "deleted"};
    long long value = zdb_command_integer(test, argvsz(argv), argv);

    if(value == 0)
        return TEST_SUCCESS;

    return TEST_FAILED_FATAL;
}

runtest_prio(120, default_exists) {
    const char *argv[] = {"EXISTS", "hello"};
    long long value = zdb_command_integer(test, argvsz(argv), argv);

    if(value == 1)
        return TEST_SUCCESS;

    return TEST_FAILED_FATAL;
}

// command: check
runtest_prio(120, default_check_missing_args) {
    const char *argv[] = {"CHECK"};
    return zdb_command_error(test, argvsz(argv), argv);
}

runtest_prio(120, default_check_notfound) {
    const char *argv[] = {"CHECK", "unknown-key"};
    return zdb_command_error(test, argvsz(argv), argv);
}

runtest_prio(120, default_check_deleted) {
    const char *argv[] = {"CHECK", "deleted"};
    return zdb_command_error(test, argvsz(argv), argv);
}


runtest_prio(120, default_check) {
    const char *argv[] = {"CHECK", "hello"};
    long long value = zdb_command_integer(test, argvsz(argv), argv);

    if(value == 1)
        return TEST_SUCCESS;

    return TEST_FAILED_FATAL;
}

