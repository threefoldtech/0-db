#ifndef ZDB_TESTS_UTILS_H
    #define ZDB_TESTS_UTILS_H

    int zdb_result(redisReply *reply, int value);
    int zdb_command(test_t *test, int argc, const char *argv[]);
    int zdb_command_error(test_t *test, int argc, const char *argv[]);
    int zdb_set(test_t *test, char *key, char *value);
    int zdb_bset(test_t *test, void *key, size_t keylen, void *payload, size_t paylen);
    int zdb_check(test_t *test, char *key, char *value);
    int zdb_bcheck(test_t *test, void *key, size_t keylen, void *payload, size_t paylen);
    int zdb_nsnew(test_t *test, char *nsname);

    int zdb_basic_check(test_t *test, const char *command);
    long long zdb_command_integer(test_t *test, int argc, const char *argv[]);
    redisReply *zdb_response_scan(test_t *test, int argc, const char *argv[]);
    redisReply *zdb_response_history(test_t *test, int argc, const char *argv[]);

    #define argvsz(x) (sizeof(x) / sizeof(char *))
#endif
