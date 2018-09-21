#include <stdio.h>
#include <string.h>
#include "tests_user.h"
#include "zdb_utils.h"
#include "tests.h"

int zdb_result(redisReply *reply, int value) {
    if(reply)
        freeReplyObject(reply);

    return value;
}

// send command which should not fails
int zdb_command(test_t *test, int argc, const char *argv[]) {
    redisReply *reply;

    if(!(reply = redisCommandArgv(test->zdb, argc, argv, NULL)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(reply->type != REDIS_REPLY_STATUS) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED);
    }

    return zdb_result(reply, TEST_SUCCESS);
}

// send command which should fails
int zdb_command_error(test_t *test, int argc, const char *argv[]) {
    redisReply *reply;

    if(!(reply = redisCommandArgv(test->zdb, argc, argv, NULL)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(reply->type != REDIS_REPLY_ERROR && reply->type != REDIS_REPLY_NIL) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED);
    }

    return zdb_result(reply, TEST_SUCCESS);
}


int zdb_set(test_t *test, char *key, char *value) {
    if(test->mode == SEQUENTIAL)
        return TEST_SKIPPED;

    redisReply *reply;

    if(!(reply = redisCommand(test->zdb, "SET %s %s", key, value)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(reply->type != REDIS_REPLY_STRING) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED_FATAL);
    }

    if(strcmp(reply->str, key)) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED_FATAL);
    }

    return zdb_result(reply, TEST_SUCCESS);
}

int zdb_set_seq(test_t *test, uint32_t key, char *value, uint32_t *response) {
    if(test->mode == USERKEY)
        return TEST_SKIPPED;

    redisReply *reply;

    if(key == SEQNEW) {
        if(!(reply = redisCommand(test->zdb, "SET %b %s", NULL, 0, value)))
            return zdb_result(reply, TEST_FAILED_FATAL);

    } else {
        if(!(reply = redisCommand(test->zdb, "SET %b %s", &key, sizeof(uint32_t), value)))
            return zdb_result(reply, TEST_FAILED_FATAL);
    }

    if(reply->type != REDIS_REPLY_STRING) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED_FATAL);
    }

    if(reply->len != sizeof(uint32_t))
        return zdb_result(reply, TEST_FAILED_FATAL);

    memcpy(response, reply->str, sizeof(uint32_t));
    log("Key ID: %u\n", *response);

    return zdb_result(reply, TEST_SUCCESS);
}


int zdb_bset(test_t *test, void *key, size_t keylen, void *payload, size_t paylen) {
    redisReply *reply;

    if(!(reply = redisCommand(test->zdb, "SET %b %b", key, keylen, payload, paylen)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(reply->type != REDIS_REPLY_STRING) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED_FATAL);
    }

    if(memcmp(reply->str, key, keylen)) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED_FATAL);
    }

    return zdb_result(reply, TEST_SUCCESS);
}


int zdb_check(test_t *test, char *key, char *value) {
    redisReply *reply;

    if(!(reply = redisCommand(test->zdb, "GET %s", key)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(reply->type != REDIS_REPLY_STRING) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED);
    }

    if(strcmp(reply->str, value)) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED_FATAL);
    }

    return zdb_result(reply, TEST_SUCCESS);
}

int zdb_bcheck(test_t *test, void *key, size_t keylen, void *payload, size_t paylen) {
    redisReply *reply;

    if(!(reply = redisCommand(test->zdb, "GET %b", key, keylen)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(reply->type != REDIS_REPLY_STRING) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED);
    }

    if(memcmp(reply->str, payload, paylen)) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED_FATAL);
    }

    return zdb_result(reply, TEST_SUCCESS);
}


int zdb_nsnew(test_t *test, char *nsname) {
    redisReply *reply;

    if(!(reply = redisCommand(test->zdb, "NSNEW %s", nsname)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(reply->type == REDIS_REPLY_ERROR) {
        if(strcmp(reply->str, "This namespace is not available") == 0) {
            log("namespace seems already exists, not an error\n");
            return zdb_result(reply, TEST_WARNING);
        }
    }

    if(reply->type != REDIS_REPLY_STATUS) {
        log("%s\n", reply->str);
        return zdb_result(reply, TEST_FAILED);
    }

    return zdb_result(reply, TEST_SUCCESS);
}

redisReply *zdb_response_scan(test_t *test, int argc, const char *argv[]) {
    redisReply *reply;

    if(!(reply = redisCommandArgv(test->zdb, argc, argv, NULL)))
        return NULL;

    if(reply->type != REDIS_REPLY_ARRAY) {
        log("%s\n", reply->str);
        freeReplyObject(reply);
        return NULL;
    }

    if(reply->elements != 2) {
        log("Unexpected array length: %lu\n", reply->elements);
        freeReplyObject(reply);
        return NULL;
    }

    return reply;
}

redisReply *zdb_response_history(test_t *test, int argc, const char *argv[]) {
    redisReply *reply;

    if(!(reply = redisCommandArgv(test->zdb, argc, argv, NULL)))
        return NULL;

    if(reply->type != REDIS_REPLY_ARRAY) {
        log("%s\n", reply->str);
        freeReplyObject(reply);
        return NULL;
    }

    if(reply->elements != 3) {
        log("Unexpected array length: %lu\n", reply->elements);
        freeReplyObject(reply);
        return NULL;
    }

    return reply;
}


long long zdb_command_integer(test_t *test, int argc, const char *argv[]) {
    redisReply *reply;

    if(!(reply = redisCommandArgv(test->zdb, argc, argv, NULL)))
        return zdb_result(reply, TEST_FAILED_FATAL);

    if(reply->type != REDIS_REPLY_INTEGER) {
        log("Not an integer\n");
        zdb_result(reply, TEST_FAILED);

        return -1; // could be false positive
    }

    long long value = reply->integer;
    zdb_result(reply, TEST_SUCCESS);

    return value;
}

// test different scenario (needs to be run after default_del_deleted)
//  - non-existing key
//  - deleted key
//  - with wrong argument count
//  - with no argument
//  - with a large key
//  - with an empty payload
int zdb_basic_check(test_t *test, const char *command) {
    log("Testing non-existing key\n");
    const char *argv1[] = {command, "non-existing"};
    if(zdb_command_error(test, argvsz(argv1), argv1) != TEST_SUCCESS)
        return TEST_FAILED_FATAL;


    log("Testing with deleted key\n");
    const char *argv2[] = {command, "deleted"};
    if(zdb_command_error(test, argvsz(argv2), argv2) != TEST_SUCCESS)
        return TEST_FAILED_FATAL;


    log("Testing wrong argument count\n");
    const char *argv3[] = {command, "hello", "world"};
    if(zdb_command_error(test, argvsz(argv3), argv3) != TEST_SUCCESS)
        return TEST_FAILED_FATAL;


    log("Testing without argument\n");
    const char *argv4[] = {command};
    if(zdb_command_error(test, argvsz(argv4), argv4) != TEST_SUCCESS)
        return TEST_FAILED_FATAL;


    log("Testing with a large key\n");
    const char lkey[512] = {0};
    memset((char *) lkey, 'x', sizeof(lkey) - 1);

    const char *argv5[] = {command, lkey};
    if(zdb_command_error(test, argvsz(argv5), argv5) != TEST_SUCCESS)
        return TEST_FAILED_FATAL;


    log("Testing with empty payload\n");
    const char *argv6[] = {command, ""};
    if(zdb_command_error(test, argvsz(argv6), argv6) != TEST_SUCCESS)
        return TEST_FAILED_FATAL;

    return TEST_SUCCESS;
}
