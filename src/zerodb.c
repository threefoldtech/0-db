#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <signal.h>
#include <execinfo.h>
#include "zerodb.h"
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

// signal handler will take care to try to
// save as much as possible, when problem occures
// for exemple, on segmentation fault, we will try to flush
// and closes descriptor anyway to avoid loosing data
static void sighandler(int signal) {
    void *buffer[1024];

    switch(signal) {
        case SIGSEGV:
            fprintf(stderr, "[-] fatal: segmentation fault\n");
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

    // creating the index in memory
    // this will returns us the id of the index
    // file currently used, this is needed by the data
    // storage to keep files linked (index-0067 <> data-0067)
    uint16_t indexid = index_init();
    data_init(indexid);

    // main worker point
    redis_listen(LISTEN_ADDR, LISTEN_PORT);

    // we should not reach this point in production
    // this case is handled when calling explicitly
    // a STOP to the server to gracefuly quit
    //
    // this is useful when profiling to ensure there
    // is no memory leaks, if everything is cleaned as
    // expected.
    index_destroy();
    data_destroy();

    return 0;
}
