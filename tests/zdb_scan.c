#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tests_user.h"
#include "zdb_utils.h"
#include "tests.h"

// sequential priority
#define sp 175

static char *namespace_scan = "test_scan";

static int scan_check(test_t *test, int argc, const char *argv[], char *expected) {
    redisReply *reply;

    if(!(reply = zdb_response_scan(test, argc, argv)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(strcmp(reply->element[0]->str, expected) == 0)
        return zdb_result(reply, TEST_SUCCESS);

    log("%s\n", reply->str);

    return zdb_result(reply, TEST_FAILED);
}

// create a new namespace
runtest_prio(sp, scan_init) {
    return zdb_nsnew(test, namespace_scan);
}

// select this new namespace
runtest_prio(sp, scan_select) {
    const char *argv[] = {"SELECT", namespace_scan};
    return zdb_command(test, argvsz(argv), argv);
}


runtest_prio(sp, scan_init_chain1) {
    return zdb_set(test, "key1", "aaaa");
}

runtest_prio(sp, scan_init_chain2) {
    return zdb_set(test, "key2", "bbbb");
}

runtest_prio(sp, scan_init_chain3) {
    return zdb_set(test, "key3", "cccc");
}

runtest_prio(sp, scan_init_chain4) {
    return zdb_set(test, "key4", "dddd");
}

runtest_prio(sp, scan_init_chain5) {
    return zdb_set(test, "key5", "eeee");
}

runtest_prio(sp, scan_init_chain6) {
    return zdb_set(test, "key6", "ffff");
}

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


