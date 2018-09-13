#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tests_user.h"
#include "zdb_utils.h"
#include "tests.h"

// sequential priority
#define sp 175

static char *namespace_history = "test_history";

static int history_check(test_t *test, int argc, const char *argv[], char *expected) {
    redisReply *reply;

    if(!(reply = zdb_response_history(test, argc, argv)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->element[2]->str, expected) == 0)
        return zdb_result(reply, TEST_SUCCESS);

    log("%s\n", reply->str);

    return zdb_result(reply, TEST_FAILED);
}

// create a new namespace
runtest_prio(sp, history_init) {
    return zdb_nsnew(test, namespace_history);
}

// select this new namespace
runtest_prio(sp, history_select) {
    const char *argv[] = {"SELECT", namespace_history};
    return zdb_command(test, argvsz(argv), argv);
}


runtest_prio(sp, history_init_chain1) {
    return zdb_set(test, "changeme", "value 1");
}

runtest_prio(sp, history_init_chain2) {
    return zdb_set(test, "changeme", "value -- 2");
}

runtest_prio(sp, history_init_chain3) {
    return zdb_set(test, "changeme", "val 3");
}

runtest_prio(sp, history_init_chain4) {
    return zdb_set(test, "changeme", "new value 4");
}

runtest_prio(sp, history_init_chain5) {
    return zdb_set(test, "changeme", "history value 5");
}

runtest_prio(sp, history_init_chain6) {
    return zdb_set(test, "changeme", "history value 6");
}

runtest_prio(sp, history_first_hit) {
    if(test->mode == SEQUENTIAL)
        return TEST_SKIPPED;

    const char *argv[] = {"HISTORY", "changeme"};
    return history_check(test, argvsz(argv), argv, "history value 6");
}


/*
// start scan test
runtest_prio(sp, scan_get_first_key) {
    const char *argv[] = {"SCAN"};
    return scan_check(test, argvsz(argv), argv, "key1");
}

runtest_prio(sp, scan_get_last_key) {
    const char *argv[] = {"RSCAN"};
    return scan_check(test, argvsz(argv), argv, "key6");
}

runtest_prio(sp, scan_get_second_key) {
    const char *argv[] = {"SCAN", "key1"};
    return scan_check(test, argvsz(argv), argv, "key2");
}

runtest_prio(sp, scan_get_last_minusone_key) {
    const char *argv[] = {"RSCAN", "key6"};
    return scan_check(test, argvsz(argv), argv, "key5");
}


runtest_prio(sp, scan_remove_first) {
    const char *argv[] = {"DEL", "key1"};
    return zdb_command(test, argvsz(argv), argv);
}

runtest_prio(sp, scan_get_new_first_key) {
    const char *argv[] = {"SCAN"};
    return scan_check(test, argvsz(argv), argv, "key2");
}


runtest_prio(sp, scan_remove_last) {
    const char *argv[] = {"DEL", "key6"};
    return zdb_command(test, argvsz(argv), argv);
}

runtest_prio(sp, scan_get_new_last_key) {
    const char *argv[] = {"RSCAN"};
    return scan_check(test, argvsz(argv), argv, "key5");
}

// scan on unknown key
runtest_prio(sp, scan_non_existing) {
    const char *argv[] = {"SCAN", "nonexisting"};
    return zdb_command_error(test, argvsz(argv), argv);
}

runtest_prio(sp, rscan_non_existing) {
    const char *argv[] = {"RSCAN", "nonexisting"};
    return zdb_command_error(test, argvsz(argv), argv);
}

// scan on deleted key
runtest_prio(sp, scan_deleted_key) {
    const char *argv[] = {"SCAN", "key1"};
    return zdb_command_error(test, argvsz(argv), argv);
}

runtest_prio(sp, rscan_deleted_key) {
    const char *argv[] = {"RSCAN", "key1"};
    return zdb_command_error(test, argvsz(argv), argv);
}

// scan from last key
runtest_prio(sp, scan_ask_after_last) {
    const char *argv[] = {"SCAN", "key5"};
    return zdb_command_error(test, argvsz(argv), argv);
}

// rscan from first key
runtest_prio(sp, scan_ask_before_first) {
    const char *argv[] = {"RSCAN", "key2"};
    return zdb_command_error(test, argvsz(argv), argv);
}
*/
