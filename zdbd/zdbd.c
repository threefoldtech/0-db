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
#include <ctype.h>
#include <time.h>
#include "libzdb.h"
#include "zdbd.h"
#include "index.h"
#include "data.h"
#include "namespace.h"
#include "filesystem.h"
#include "hook.h"
#include "redis.h"

//
// global system settings
//
zdbd_settings_t zdbd_rootsettings = {
    .listen = ZDBD_DEFAULT_LISTENADDR,
    .port = ZDBD_DEFAULT_PORT,
    .verbose = 0,
    .adminpwd = NULL,
    .socket = NULL,
    .background = 0,
    .logfile = NULL,
    .protect = 0,
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
    {"datasize",   required_argument, 0, 'D'},
    {"maxsize",    required_argument, 0, 'M'},
    {"protect",    no_argument,       0, 'P'},
    {"help",       no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

// debug tools
static char __hex[] = "0123456789abcdef";

void zdbd_fulldump(void *_data, size_t len) {
    uint8_t *data = _data;
    unsigned int i, j;

    printf("[*] data fulldump [%p -> %p] (%lu bytes)\n", data, data + len, len);
    printf("[*] 0x0000: ");

    for(i = 0; i < len; ) {
        printf("%02x ", data[i++]);

        if(i % 16 == 0) {
            printf("|");

            for(j = i - 16; j < i; j++)
                printf("%c", ((isprint(data[j]) ? data[j] : '.')));

            printf("|\n[*] 0x%04x: ", i);
        }
    }

    if(i % 16) {
        printf("%-*s |", 5 * (16 - (i % 16)), " ");

        for(j = i - (i % 16); j < len; j++)
            printf("%c", ((isprint(data[j]) ? data[j] : '.')));

        printf("%-*s|\n", 16 - ((int) len % 16), " ");
    }

    printf("\n");
}

void zdbd_hexdump(void *input, size_t length) {
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

static uint32_t instanceid() {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    srand((time_t) ts.tv_nsec);

    // generating random id, greater than zero
    return (uint32_t) ((rand() % (1 << 30)) + 1);
}

//
// global warning and fatal message
//
void *zdbd_warnp(char *str) {
    fprintf(stderr, "[-] %s: %s\n", str, strerror(errno));
    return NULL;
}

void zdbd_verbosep(char *prefix, char *str) {
#ifdef RELEASE
    // only match on verbose flag if we are
    // in release mode, otherwise do always the
    // print, we are in debug mode anyway
    if(!zdbd_rootsettings.verbose)
        return;
#endif

    fprintf(stderr, "[-] %s: %s: %s\n", prefix, str, strerror(errno));
}

void zdbd_diep(char *str) {
    zdbd_warnp(str);
    exit(EXIT_FAILURE);
}

static int signal_intercept(int signal, void (*function)(int)) {
    struct sigaction sig;
    int ret;

    sigemptyset(&sig.sa_mask);
    sig.sa_handler = function;
    sig.sa_flags = 0;

    if((ret = sigaction(signal, &sig, NULL)) == -1)
        zdbd_diep("sigaction");

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

            if(zdb_rootsettings.hook) { // FIXME
                hook_t *hook = hook_new("crash", 1);
                hook_append(hook, zdb_rootsettings.zdbid ? zdb_rootsettings.zdbid : "unknown-id"); // FIXME
                hook_execute(hook);
                hook_free(hook);
            }

            // trying to save what we can save
            printf("\n[+] signal: crashed, trying to clean\n");
            namespaces_emergency();
            break;

        case SIGINT:
        case SIGTERM:
            printf("\n[+] signal: request cleaning\n");

            if(zdb_rootsettings.hook) {
                hook_t *hook = hook_new("close", 1);
                hook_append(hook, zdb_rootsettings.zdbid ? zdb_rootsettings.zdbid : "unknown-id"); // FIXME
                hook_execute(hook);
                hook_free(hook);
            }

            namespaces_emergency();
            break;
    }

    // forwarding original error code
    exit(128 + signal);
}

static void zdbid_set(char *listenaddr, int port, char *socket) {
    if(socket) {
        // unix socket
        if(asprintf(&zdb_rootsettings.zdbid, "unix://%s", socket) < 0) // FIXME
            zdbd_diep("asprintf");

        return;
    }

    // default tcp
    if(asprintf(&zdb_rootsettings.zdbid, "tcp://%s:%d", listenaddr, port) < 0) // FIXME
        zdbd_diep("asprintf");
}

static int proceed(zdb_settings_t *zdb_settings, zdbd_settings_t *zdbd_settings) {
    zdbd_verbose("[+] system: setting up environments\n");
    signal_intercept(SIGSEGV, sighandler);
    signal_intercept(SIGINT, sighandler);
    signal_intercept(SIGTERM, sighandler);
    signal(SIGCHLD, SIG_IGN);

    zdbid_set(zdbd_settings->listen, zdbd_settings->port, zdbd_settings->socket);

    // namespace is the root of the whole index/data system
    // anything related to data is always attached to at least
    // one namespace (the default) one, and all the others
    // are based on a fork of namespace
    //
    // the namespace system will take care about all the loading
    // and the destruction
    namespaces_init(zdb_settings);

    // apply global protected flag to the default namespace
    if(zdbd_settings->protect) {
        namespace_t *defns = namespace_get_default();
        defns->password = strdup(zdbd_settings->adminpwd);
    }

    // apply global maximum size for the global namespace
    if(zdb_settings->maxsize) {
        namespace_t *defns = namespace_get_default();
        defns->maxsize = zdb_settings->maxsize;
    }

    // main worker point (if dump not enabled)
    if(!zdb_settings->dump)
        redis_listen(zdbd_settings->listen, zdbd_settings->port, zdbd_settings->socket);

    // we should not reach this point in production
    // this case is handled when calling explicitly
    // a STOP to the server to gracefuly quit
    //
    // this is useful when profiling to ensure there
    // is no memory leaks, if everything is cleaned as
    // expected.
    namespaces_destroy();

    // FIXME
    free(zdb_settings->zdbid);
    zdb_settings->zdbid = NULL;

    return 0;
}

void usage() {
    printf("Command line arguments:\n\n");

    printf(" Database settings:\n");
    printf("  --data  <dir>       datafile directory (default " ZDB_DEFAULT_DATAPATH ")\n");
    printf("  --index <dir>       indexfiles directory (default " ZDB_DEFAULT_INDEXPATH ")\n");
    printf("  --mode  <mode>      select working mode:\n");
    printf("                       > user: default user key-value mode\n");
    printf("                       > seq: sequential keys generated\n");
    printf("                       > direct: direct position by key\n");
    printf("                       > block: fixed blocks length (smaller direct)\n");
    printf("  --datasize <size>   maximum datafile size before split (default: %.2f MB)\n\n", MB(ZDB_DEFAULT_DATA_MAXSIZE));

    printf(" Network options:\n");
    printf("  --listen <addr>     listen address (default " ZDBD_DEFAULT_LISTENADDR ")\n");
    printf("  --port   <port>     listen port (default %d)\n", ZDBD_DEFAULT_PORT);
    printf("  --socket <path>     unix socket path (override listen and port)\n\n");

    printf(" Administrative:\n");
    printf("  --hook     <file>   execute external hook script\n");
    printf("  --admin    <pass>   set admin password\n");
    printf("  --maxsize  <size>   set default namespace maximum datasize (in bytes)\n");
    printf("  --protect           set default namespace protected by admin password\n\n");

    printf(" Useful tools:\n");
    printf("  --verbose           enable verbose (debug) information\n");
    printf("  --dump              only dump index contents, then exit (debug)\n");
    printf("  --sync              force all write to be sync'd\n");
    printf("  --background        run in background (daemon), when ready\n");
    printf("  --logfile <file>    log file (only in daemon mode)\n");
    printf("  --help              print this message\n");

    exit(EXIT_FAILURE);
}

//
// main entry: processing arguments
//
int main(int argc, char *argv[]) {
    zdbd_notice("[*] Zero-DB (0-db), v" ZDB_VERSION " (commit " REVISION ")");


    zdb_settings_t *zdb_settings = &zdb_rootsettings;
    zdbd_settings_t *zdbd_settings = &zdbd_rootsettings;

    int option_index = 0;

    while(1) {
        int i = getopt_long_only(argc, argv, "", long_options, &option_index);

        if(i == -1)
            break;

        switch(i) {
            case 'd':
                zdb_settings->datapath = optarg;
                break;

            case 'i':
                zdb_settings->indexpath = optarg;
                break;

            case 'l':
                zdbd_settings->listen = optarg;
                break;

            case 'p':
                zdbd_settings->port = atoi(optarg);
                break;

            case 'v':
                zdb_settings->verbose = 1;
                zdbd_settings->verbose = 1;
                zdbd_verbose("[+] system: verbose mode enabled\n");
                break;

            case 'b':
                zdbd_settings->background = 1;
                zdbd_verbose("[+] system: background fork enabled\n");
                break;

            case 'o':
                zdbd_settings->logfile = optarg;
                break;

            case 'x':
                zdb_settings->dump = 1;
                break;

            case 's':
                zdb_settings->sync = 1;
                break;

            case 'k':
                zdb_settings->hook = optarg;
                zdbd_debug("[+] system: external hook: %s\n", zdb_settings->hook);
                break;

            case 't':
                zdb_settings->synctime = atoi(optarg);
                break;

            case 'a':
                zdbd_settings->adminpwd = optarg;
                zdbd_verbose("[+] system: admin password set\n");
                break;

            case 'P':
                zdbd_settings->protect = 1;
                zdbd_verbose("[+] system: protected database enabled\n");
                break;

            case 'M':
                zdb_settings->maxsize = atol(optarg);
                zdbd_verbose("[+] system: default namespace maxsize: %.2f MB\n", MB(zdb_settings->maxsize));
                break;

            case 'm':
                if(strcmp(optarg, "user") == 0) {
                    zdb_settings->mode = KEYVALUE;

                } else if(strcmp(optarg, "seq") == 0) {
                    zdb_settings->mode = SEQUENTIAL;

                } else if(strcmp(optarg, "direct") == 0) {
                    // settings->mode = DIRECTKEY;
                    zdb_settings->mode = SEQUENTIAL;

                    zdbd_warning("[!] WARNING: direct mode doesn't exists anymore !");
                    zdbd_warning("[!] WARNING: this mode is replaced by 'sequential' mode");
                    zdbd_warning("[!] WARNING: which works the same way, but offers more");
                    zdbd_warning("[!] WARNING: flexibility and performance");
                    zdbd_warning("[!] WARNING: ");
                    zdbd_warning("[!] WARNING: direct mode will not be supported at all anymore");
                    zdbd_warning("[!] WARNING: in futur release");

                } else if(strcmp(optarg, "block") == 0) {
                    zdb_settings->mode = DIRECTBLOCK;

                } else {
                    zdbd_danger("[-] invalid mode '%s'", optarg);
                    fprintf(stderr, "[-] mode 'user', 'seq' or 'direct' expected\n");
                    exit(EXIT_FAILURE);
                }

                break;

            case 'u':
                zdbd_settings->socket = optarg;
                break;

            case 'D':
                zdb_settings->datasize = atol(optarg);
                size_t maxsize = 0xffffffff;

                // maximum 4 GB (32 bits) allowed
                if(zdb_settings->datasize >= maxsize) {
                    zdbd_danger("[-] datasize cannot be larger than %lu bytes (%.0f MB)", maxsize, MB(maxsize));
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

    if(zdbd_settings->protect && !zdbd_settings->adminpwd) {
        zdbd_danger("[-] protected mode only works with admin password");
        exit(EXIT_FAILURE);
    }

    //
    // print information relative to database instance
    //
    printf("[+] system: running mode: " COLOR_GREEN "%s" COLOR_RESET "\n", zdb_modes[zdb_settings->mode]);

    // max files is limited by type length of dataid, which is uint16 by default
    // taking field size in bytes, multiplied by 8 for bits
    size_t maxfiles = 1 << sizeof(((data_root_t *) 0)->dataid) * 8;

    // max database size is maximum datafile size multiplied by amount of files
    uint64_t maxsize = maxfiles * zdb_settings->datasize;

    zdbd_verbose("[+] system: maximum namespace size: %.2f GB\n", GB(maxsize));

    //
    // ensure default directories
    // for a fresh start if this is a new instance
    //
    if(!dir_exists(zdb_settings->datapath)) {
        zdbd_verbose("[+] system: creating datapath: %s\n", zdb_settings->datapath);
        dir_create(zdb_settings->datapath);
    }

    if(!dir_exists(zdb_settings->indexpath)) {
        zdbd_verbose("[+] system: creating indexpath: %s\n", zdb_settings->indexpath);
        dir_create(zdb_settings->indexpath);
    }

    // generating instance id // FIXME
    // zdb_settings->iid = instanceid();
    // zdbd_verbose("[+] system: instance id: %u\n", settings->iid);

    // initialize statistics // FIXME
    memset(&zdb_settings->stats, 0x00, sizeof(zdb_stats_t));
    zdb_settings->stats.boottime = time(NULL); // FIXME

    // let's go
    return proceed(zdb_settings, zdbd_settings);
}
