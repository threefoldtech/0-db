#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tests_user.h"
#include "zdb_utils.h"
#include "tests.h"

// sequential priority
#define sp 170

static char *namespace_payload = "test_payload";
static size_t sizes_payload[] = {
    512,               // 512 bytes
    1024,              // 1 KB
    4 * 1024,          // 4 KB
    64 * 1024,         // 64 KB
    512 * 1024,        // 512 KB
    1024 * 1024,       // 1 MB
    2 * 1024 * 1024,   // 2 MB
    4 * 1024 * 1024,   // 4 MB
    8 * 1024 * 1024,   // 8 MB
};

#define cmdptr  int (*command)(test_t *, void *, size_t, void *, size_t)

int payload_execute(test_t *test, char *key, size_t keylen, size_t length, cmdptr) {
    char *payload;

    if(!(payload = malloc(length)))
        return TEST_FAILED_FATAL;

    memset(payload, 0x42, length);

    int response = command(test, key, keylen, payload, length);

    free(payload);
    return response;

}

int set_fixed_payload(test_t *test, size_t index) {
    char key[64];
    size_t keylen;

    if(test->mode == USERKEY) {
        sprintf(key, "data-%lu", sizes_payload[index]);
        keylen = strlen(key);
    }

    if(test->mode == SEQUENTIAL) {
        memset(key, 0x00, sizeof(key));
        keylen = 0;
    }

    return payload_execute(test, key, keylen, sizes_payload[index], zdb_bset);
}

int get_fixed_payload(test_t *test, uint32_t index) {
    char key[64];
    size_t keylen ;

    if(test->mode == USERKEY) {
        sprintf(key, "data-%lu", sizes_payload[index]);
        keylen = strlen(key);
    }

    if(test->mode == SEQUENTIAL) {
        memcpy(key, &index, sizeof(uint32_t));
        keylen = sizeof(uint32_t);
    }

    return payload_execute(test, key, keylen, sizes_payload[index], zdb_bcheck);
}


// create a new namespace
runtest_prio(sp, payload_init) {
    return zdb_nsnew(test, namespace_payload);
}

// select this new namespace
runtest_prio(sp, payload_select) {
    const char *argv[] = {"SELECT", namespace_payload};
    return zdb_command(test, argvsz(argv), argv);
}


// set different datasize, know payload
runtest_prio(sp, payload_set_512b) {
    return set_fixed_payload(test, 0);
}

runtest_prio(sp, payload_set_1k) {
    return set_fixed_payload(test, 1);
}

runtest_prio(sp, payload_set_4k) {
    return set_fixed_payload(test, 2);
}

runtest_prio(sp, payload_set_64k) {
    return set_fixed_payload(test, 3);
}

runtest_prio(sp, payload_set_512k) {
    return set_fixed_payload(test, 4);
}

runtest_prio(sp, payload_set_1m) {
    return set_fixed_payload(test, 5);
}

runtest_prio(sp, payload_set_2m) {
    return set_fixed_payload(test, 6);
}

runtest_prio(sp, payload_set_4m) {
    return set_fixed_payload(test, 7);
}

runtest_prio(sp, payload_set_8m) {
    return set_fixed_payload(test, 8);
}

/*
// client is disconnected if payload is too big
//
// this test should fail (limit is set to 8 MB)
runtest_prio(sp, payload_set_10m_fail) {
    int response = set_fixed_payload(test, 10 * 1024 * 1024);
    if(response == TEST_FAILED_FATAL)
        return TEST_SUCCESS;

    return TEST_FAILED;
}
*/


// read the differents key sets and ensure response
runtest_prio(sp, payload_get_512b) {
    return get_fixed_payload(test, 0);
}

runtest_prio(sp, payload_get_1k) {
    return get_fixed_payload(test, 1);
}

runtest_prio(sp, payload_get_4k) {
    return get_fixed_payload(test, 2);
}

runtest_prio(sp, payload_get_64k) {
    return get_fixed_payload(test, 3);
}

runtest_prio(sp, payload_get_512k) {
    return get_fixed_payload(test, 4);
}

runtest_prio(sp, payload_get_1m) {
    return get_fixed_payload(test, 5);
}

runtest_prio(sp, payload_get_2m) {
    return get_fixed_payload(test, 6);
}

runtest_prio(sp, payload_get_4m) {
    return get_fixed_payload(test, 7);
}

runtest_prio(sp, payload_get_8m) {
    return get_fixed_payload(test, 8);
}


