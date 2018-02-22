#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <signal.h>
#include <execinfo.h>
#include <getopt.h>
#include "zerodb.h"
#include "index.h"
#include "data.h"
#include "namespace.h"
#include "redis.h"
#include "filesystem.h"

//
// global system settings
//
settings_t rootsettings = {
    .datapath = "./zdb-data",
    .indexpath = "./zdb-index",
    .listen = "0.0.0.0",
    .port = 9900,
    .verbose = 0,
    .dump = 0,
    .sync = 0,
    .synctime = 0,
    .mode = KEYVALUE,
};

static struct option long_options[] = {
    {"data",      required_argument, 0, 'd'},
    {"index",     required_argument, 0, 'i'},
    {"listen",    required_argument, 0, 'l'},
    {"port",      required_argument, 0, 'p'},
    {"verbose",   no_argument,       0, 'v'},
    {"sync",      no_argument,       0, 's'},
    {"synctime",  required_argument, 0, 't'},
    {"dump",      no_argument,       0, 'x'},
    {"mode",      required_argument, 0, 'm'},
    {"help",      no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

static char *modes[] = {
    "default key-value",
    "sequential keys",
    "direct key position",
};

// debug tools
static char __hex[] = "0123456789abcdef";

void hexdump(void *input, size_t length) {
    unsigned char *buffer = (unsigned char *) input;
    char *output = calloc((length * 2) + 1, 1);
    char *writer = output;

    for(unsigned int i = 0, j = 0; i < length; i++, j += 2) {
        *writer++ = __hex[(buffer[i] & 0xF0) >> 4];
        *writer++ = __hex[buffer[i] & 0x0F];
    }

    printf("0x%s", output);
    free(output);
}

//
// global warning and fatal message
//
void *warnp(char *str) {
    fprintf(stderr, "[-] %s: %s\n", str, strerror(errno));
    return NULL;
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
    sig.sa_flags = 0;

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
            printf("\n[+] signal: request cleaning\n");
            namespace_emergency();

        break;
    }

    // forwarding original error code
    exit(128 + signal);
}


static int proceed(struct settings_t *settings) {
    verbose("[+] system: setting up environments\n");
    signal_intercept(SIGSEGV, sighandler);
    signal_intercept(SIGINT, sighandler);

    // namespace is the root of the whole index/data system
    // anything related to data is always attached to at least
    // one namespace (the default) one, and all the others
    // are based on a fork of namespace
    //
    // the namespace system will take care about all the loading
    // and the destruction
    namespace_init(settings);

    // main worker point
    redis_listen(settings->listen, settings->port);

    // we should not reach this point in production
    // this case is handled when calling explicitly
    // a STOP to the server to gracefuly quit
    //
    // this is useful when profiling to ensure there
    // is no memory leaks, if everything is cleaned as
    // expected.
    namespace_destroy();

    return 0;
}

void usage() {
    printf("Command line arguments:\n");
    printf("  --data      datafile directory (default ./data)\n");
    printf("  --index     indexfiles directory (default ./index)\n");
    printf("  --listen    listen address (default 0.0.0.0)\n");
    printf("  --port      listen port (default 9900)\n");
    printf("  --verbose   enable verbose (debug) information\n");
    printf("  --dump      only dump index contents (debug)\n");
    printf("  --sync      force all write to be sync'd\n");
    printf("  --mode      select mode:\n");
    printf("               > user: default user key-value mode\n");
    printf("               > seq: sequential keys generated\n");
    printf("               > direct: direct position by key\n");
    printf("  --help      print this message\n");

    exit(EXIT_FAILURE);
}

//
// main entry: processing arguments
//
int main(int argc, char *argv[]) {
    notice("[*] Zero-DB (0-db), revision " REVISION);

    settings_t *settings = &rootsettings;
    int option_index = 0;

    while(1) {
        // int i = getopt_long_only(argc, argv, "d:i:l:p:vxh", long_options, &option_index);
        int i = getopt_long_only(argc, argv, "", long_options, &option_index);

        if(i == -1)
            break;

        switch(i) {
            case 'd':
                settings->datapath = optarg;
                break;

            case 'i':
                settings->indexpath = optarg;
                break;

            case 'l':
                settings->listen = optarg;
                break;

            case 'p':
                settings->port = atoi(optarg);
                break;

            case 'v':
                settings->verbose = 1;
                verbose("[+] system: verbose mode enabled\n");
                break;

            case 'x':
                settings->dump = 1;
                break;

            case 's':
                settings->sync = 1;
                break;

            case 't':
                settings->synctime = atoi(optarg);
                break;

            case 'm':
                if(strcmp(optarg, "user") == 0) {
                    settings->mode = KEYVALUE;

                } else if(strcmp(optarg, "seq") == 0) {
                    settings->mode = SEQUENTIAL;

                } else if(strcmp(optarg, "direct") == 0) {
                    settings->mode = DIRECTKEY;

                } else {
                    danger("[-] invalid mode '%s'", optarg);
                    fprintf(stderr, "[-] mode 'user', 'seq' or 'direct' expected\n");
                    exit(EXIT_FAILURE);
                }

                break;

            case 'h':
                usage();

            case '?':
            default:
               exit(EXIT_FAILURE);
        }
    }

    printf("[+] system: running mode: %s\n", modes[settings->mode]);

    if(!dir_exists(settings->datapath)) {
        verbose("[+] system: creating datapath: %s\n", settings->datapath);
        dir_create(settings->datapath);
    }

    if(!dir_exists(settings->indexpath)) {
        verbose("[+] system: creating indexpath: %s\n", settings->indexpath);
        dir_create(settings->indexpath);
    }

    return proceed(settings);
}
