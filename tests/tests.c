#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include "tests_user.h"
#include "tests.h"

static char *project = "0-db";

static registered_tests_t tests = {
    .length = 0,
    .longest = 0,
    .list = {},

    .success = 0,
    .failed = 0,
    .failed_fatal = 0,
    .warning = 0,
};

// register a function as runtest
// insert the function in the run list
void tests_register(char *name, int (*func)(test_t *)) {
    // printf("[+] registering: %s (%p)\n", name, func);

    tests.list[tests.length].name = name;
    tests.list[tests.length].test = func;

    if(strlen(name) > tests.longest)
        tests.longest = strlen(name);

    tests.length += 1;
}

void testsuite(test_t *maintest) {
    for(unsigned int i = 0; i < tests.length; i++) {
        runtest_t *test = &tests.list[i];

        printf("[+] >> " CYAN("%s") ": running\n", test->name);
        test->result = test->test(maintest);

        switch(test->result) {
            case TEST_SUCCESS:
                printf("[+] >> " CYAN("%s") ": " GREEN("success") "\n", test->name);
                tests.success += 1;
                break;

            case TEST_FAILED_FATAL:
                printf("[-] >> " CYAN("%s") ": " RED("failed (fatal)") "\n", test->name);
                tests.failed += 1;
                tests.failed_fatal += 1;
                break;

            case TEST_FAILED:
                printf("[-] >> " CYAN("%s") ": " RED("failed") "\n", test->name);
                tests.failed += 1;
                break;

            case TEST_WARNING:
                printf("[-] >> " CYAN("%s") ": " YELLOW("warning") "\n", test->name);
                tests.warning += 1;
                break;

            case TEST_SKIPPED:
                printf("[-] >> " CYAN("%s") ": " GREY("skipped") "\n", test->name);
                break;
        }
    }

}

static test_t settings;

int initialize_tcp() {
    settings.host = "localhost";
    settings.port = 9900;

    settings.zdb = redisConnect(settings.host, settings.port);
    settings.type = CONNECTION_TYPE_TCP;

    if(!settings.zdb || settings.zdb->err) {
        const char *error = (settings.zdb->err) ? settings.zdb->errstr : "memory error";
        log("%s:%d: %s\n", settings.host, settings.port, error);
        return 1;
    }

    return 0;
}

void initialize() {
    char *socket = "/tmp/zdb.sock";

    settings.zdb = redisConnectUnix(socket);
    settings.type = CONNECTION_TYPE_UNIX;

    if(!settings.zdb || settings.zdb->err) {
        const char *error = (settings.zdb->err) ? settings.zdb->errstr : "memory error";
        log("%s: %s\n", socket, error);

        if(initialize_tcp())
            exit(EXIT_FAILURE);
    }

    signal(SIGPIPE, SIG_IGN);
}

int main(int argc, char *argv[]) {
    (void) argc;
    (void) argv;

    printf("[+] initializing " CYAN("%s") " tests suite\n", project);
    printf("[+] tests registered: %u\n", tests.length);
    printf("[+] \n");

    for(unsigned int i = 0; i < tests.length; i++) {
        runtest_t *test = &tests.list[i];
        printf("[+]   % 4d) %-*s [%p]\n", i + 1, tests.longest + 2, test->name, test->test);
    }

    printf("[+] \n");
    printf("[+] preparing tests\n");
    initialize();

    printf("[+] running tests\n");
    printf("[+]\n");
    testsuite(&settings);

    printf("[+]\n");
    printf("[+] all tests done, summary:\n");
    printf("[+]\n");
    printf("[+]   " GREEN("success") ": %u\n", tests.success);
    printf("[+]   " RED("failed") " : %u (%u fatal)\n", tests.failed, tests.failed_fatal);
    printf("[+]   " YELLOW("warning") ": %u\n", tests.warning);
    printf("[+]\n");

    return 0;
}
