#define _GNU_SOURCE
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
    .datapath = ZDB_DEFAULT_DATAPATH,
    .indexpath = ZDB_DEFAULT_INDEXPATH,
    .listen = ZDB_DEFAULT_LISTENADDR,
    .port = ZDB_DEFAULT_PORT,
    .verbose = 0,
    .dump = 0,
    .sync = 0,
    .synctime = 0,
    .mode = KEYVALUE,
    .adminpwd = NULL,
    .socket = NULL,
    .background = 0,
    .logfile = NULL,
    .hook = NULL,
    .zdbid = NULL,
};

static struct option long_options[] = {
    {"data",       required_argument, 0, 'd'},
    {"index",      required_argument, 0, 'i'},
    {"listen",     required_argument, 0, 'l'},
    {"port",       required_argument, 0, 'p'},
    {"socket",     required_argument, 0, 'u'},
    {"verbose",    no_argument,       0, 'v'},
    {"sync",       no_argument,       0, 's'},
    {"synctime",   required_argument, 0, 't'},
    {"dump",       no_argument,       0, 'x'},
    {"mode",       required_argument, 0, 'm'},
    {"background", no_argument,       0, 'b'},
    {"logfile",    required_argument, 0, 'o'},
    {"admin",      required_argument, 0, 'a'},
    {"hook",       required_argument, 0, 'k'},
    {"help",       no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

static char *modes[] = {
    "default key-value",
    "sequential keys",
    "direct key position",
    "direct key fixed block length",
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
        case SIGTERM:
            printf("\n[+] signal: request cleaning\n");
            namespaces_emergency();

        break;
    }

    // forwarding original error code
    exit(128 + signal);
}

static void zdbid_set(char *listenaddr, int port, char *socket) {
    if(socket) {
        // unix socket
        asprintf(&rootsettings.zdbid, "unix://%s", socket);
        return;
    }

    // default tcp
    asprintf(&rootsettings.zdbid, "tcp://%s:%d", listenaddr, port);
}

static int proceed(struct settings_t *settings) {
    verbose("[+] system: setting up environments\n");
    signal_intercept(SIGSEGV, sighandler);
    signal_intercept(SIGINT, sighandler);
    signal_intercept(SIGTERM, sighandler);
    signal(SIGCHLD, SIG_IGN);

    zdbid_set(settings->listen, settings->port, settings->socket);

    // namespace is the root of the whole index/data system
    // anything related to data is always attached to at least
    // one namespace (the default) one, and all the others
    // are based on a fork of namespace
    //
    // the namespace system will take care about all the loading
    // and the destruction
    namespaces_init(settings);

    // main worker point (if dump not enabled)
    if(!settings->dump)
        redis_listen(settings->listen, settings->port, settings->socket);

    // we should not reach this point in production
    // this case is handled when calling explicitly
    // a STOP to the server to gracefuly quit
    //
    // this is useful when profiling to ensure there
    // is no memory leaks, if everything is cleaned as
    // expected.
    namespaces_destroy();

    return 0;
}

void usage() {
    printf("Command line arguments:\n");
    printf("  --data        datafile directory (default " ZDB_DEFAULT_DATAPATH ")\n");
    printf("  --index       indexfiles directory (default " ZDB_DEFAULT_INDEXPATH ")\n");
    printf("  --listen      listen address (default " ZDB_DEFAULT_LISTENADDR ")\n");
    printf("  --port        listen port (default %d)\n", ZDB_DEFAULT_PORT);
    printf("  --socket      unix socket path (override listen and port)\n");
    printf("  --verbose     enable verbose (debug) information\n");
    printf("  --dump        only dump index contents, then exit (debug)\n");
    printf("  --sync        force all write to be sync'd\n");
    printf("  --background  run in background (daemon), when ready\n");
    printf("  --logfile     log file (only in daemon mode)\n");
    printf("  --mode        select mode:\n");
    printf("                 > user: default user key-value mode\n");
    printf("                 > seq: sequential keys generated\n");
    printf("                 > direct: direct position by key\n");
    printf("                 > block: fixed blocks length (smaller direct)\n");
    printf("  --admin       set admin password\n");
    printf("  --help        print this message\n");

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

            case 'b':
                settings->background = 1;
                verbose("[+] system: background fork enabled\n");
                break;

            case 'o':
                settings->logfile = optarg;
                break;

            case 'x':
                settings->dump = 1;
                break;

            case 's':
                settings->sync = 1;
                break;

            case 'k':
                settings->hook = optarg;
                debug("[+] system: external hook: %s\n", settings->hook);
                break;

            case 't':
                settings->synctime = atoi(optarg);
                break;

            case 'a':
                settings->adminpwd = optarg;
                verbose("[+] system: admin password set\n");
                break;

            case 'm':
                if(strcmp(optarg, "user") == 0) {
                    settings->mode = KEYVALUE;

                } else if(strcmp(optarg, "seq") == 0) {
                    settings->mode = SEQUENTIAL;

                } else if(strcmp(optarg, "direct") == 0) {
                    settings->mode = DIRECTKEY;

                } else if(strcmp(optarg, "block") == 0) {
                    settings->mode = DIRECTBLOCK;

                } else {
                    danger("[-] invalid mode '%s'", optarg);
                    fprintf(stderr, "[-] mode 'user', 'seq' or 'direct' expected\n");
                    exit(EXIT_FAILURE);
                }

                break;

            case 'u':
                settings->socket = optarg;
                break;

            case 'h':
                usage();

            case '?':
            default:
               exit(EXIT_FAILURE);
        }
    }

    printf("[+] system: running mode: " COLOR_GREEN "%s" COLOR_RESET "\n", modes[settings->mode]);

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
