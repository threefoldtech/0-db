#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <signal.h>
#include <execinfo.h>
#include "rkv.h"
#include "redis.h"
#include "index.h"
#include "data.h"

void warnp(char *str) {
    fprintf(stderr, "[-] %s: %s\n", str, strerror(errno));
}

void diep(char *str) {
    warnp(str);
    exit(EXIT_FAILURE);
}

static int signal_intercept(int signal, void (*function)(int)) {
    struct sigaction sig;
    int ret;

    sigemptyset(&sig.sa_mask);
    sig.sa_handler = function;
    sig.sa_flags   = 0;

    if((ret = sigaction(signal, &sig, NULL)) == -1)
        diep("sigaction");

    return ret;
}

static void sighandler(int signal) {
    void *buffer[1024];

    switch(signal) {
        case SIGSEGV:
            fprintf(stderr, "[-] segmentation fault, flushing what's possible\n");
            fprintf(stderr, "[-] ----------------------------------\n");

            int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *));
			backtrace_symbols_fd(buffer, calls, 1);

            fprintf(stderr, "[-] ----------------------------------");

            // no break, we will execute SIGINT handler in SIGSEGV
            // which will try to save and flush buffers

        case SIGINT:
            printf("\n[+] flushing index and data\n");
            index_emergency();
            data_emergency();

        break;
    }

    // forwarding original error code
    exit(128 + signal);
}


int main(void) {
    printf("[+] initializing\n");
    signal_intercept(SIGSEGV, sighandler);
    signal_intercept(SIGINT, sighandler);

    uint16_t indexid = index_init();
    data_init(indexid);

    redis_listen(LISTEN_ADDR, LISTEN_PORT);

    index_destroy();
    data_destroy();

    return 0;
}
