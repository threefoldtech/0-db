#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tests_user.h"
#include "zdb_utils.h"
#include "tests.h"

// sequential priority
#define sp 170

static char *namespace_payload = "test_payload";

// create a new namespace
runtest_prio(sp, payload_init) {
    return zdb_nsnew(test, namespace_payload);
}

// select this new namespace
runtest_prio(sp, payload_select) {
    const char *argv[] = {"SELECT", namespace_payload};
    return zdb_command(test, argvsz(argv), argv);
}

int set_fixed_payload(test_t *test, size_t length) {
    char key[64];
    char *payload;

    sprintf(key, "data-%lu", length);

    if(!(payload = malloc(length)))
        return TEST_FAILED_FATAL;

    memset(payload, 0x42, length);

    int response = zdb_bset(test, key, strlen(key), payload, length);

    free(payload);
    return response;
}

// set different datasize, know payload
runtest_prio(sp, payload_set_512b) {
    return set_fixed_payload(test, 512);
}

runtest_prio(sp, payload_set_1k) {
    return set_fixed_payload(test, 1024);
}

runtest_prio(sp, payload_set_4k) {
    return set_fixed_payload(test, 4096);
}

runtest_prio(sp, payload_set_64k) {
    return set_fixed_payload(test, 64 * 1024);
}

runtest_prio(sp, payload_set_512k) {
    return set_fixed_payload(test, 512 * 1024);
}

runtest_prio(sp, payload_set_1m) {
    return set_fixed_payload(test, 1024 * 1024);
}

runtest_prio(sp, payload_set_2m) {
    return set_fixed_payload(test, 2 * 1024 * 1024);
}

runtest_prio(sp, payload_set_4m) {
    return set_fixed_payload(test, 4 * 1024 * 1024);
}

runtest_prio(sp, payload_set_8m) {
    return set_fixed_payload(test, 8 * 1024 * 1024);
}

// this test should fail (limit is set to 8 MB)
runtest_prio(sp, payload_set_10m_fail) {
    int response = set_fixed_payload(test, 10 * 1024 * 1024);
    if(response == TEST_FAILED_FATAL)
        return TEST_SUCCESS;

    return TEST_FAILED;
}


