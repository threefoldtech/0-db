#ifndef ZDB_TESTS_UTILS_H
    #define ZDB_TESTS_UTILS_H

    int zdb_result(redisReply *reply, int value);
    int zdb_command(test_t *test, int argc, const char *argv[]);
    int zdb_command_error(test_t *test, int argc, const char *argv[]);
    int zdb_set(test_t *test, char *key, char *value);
    int zdb_check(test_t *test, char *key, char *value);

    #define argvsz(x) (sizeof(x) / sizeof(char *))
#endif
