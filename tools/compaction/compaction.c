#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <x86intrin.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <time.h>
#include <getopt.h>
#include "zerodb.h"
#include "index.h"
#include "data.h"
#include "namespace.h"
#include "compaction.h"
#include "validity.h"

static struct option long_options[] = {
    {"data",       required_argument, 0, 'd'},
    {"index",      required_argument, 0, 'i'},
    {"namespace",  required_argument, 0, 'n'},
    {"help",       no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

void *warnp(char *str) {
    fprintf(stderr, "[-] %s: %s\n", str, strerror(errno));
    return NULL;
}

void diep(char *str) {
    warnp(str);
    exit(EXIT_FAILURE);
}

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

int namespace_compaction(compaction_t *compaction) {
    char filename[512];
    settings_t zdbsettings = {
        .datapath = compaction->datapath,
        .indexpath = compaction->indexpath,
    };

    // allocate namespaces root object
    ns_root_t *nsroot = namespaces_allocate(&zdbsettings);

    // load our single namespace
    namespace_t *namespace = namespace_load_light(nsroot, compaction->namespace);

    uint64_t maxfiles = (1 << (sizeof(((data_root_t *) 0)->dataid) * 8));

    for(size_t fileid = 0; fileid < maxfiles; fileid++) {
        snprintf(filename, sizeof(filename), "%s/zdb-data-%05lu", namespace->datapath, fileid);
        printf("[+] opening file: %s\n", filename);


    }

    return 0;
}

void usage() {
    printf("Compaction tool arguments:\n\n");

    printf("  --data      <dir>      datafile directory (root path)\n");
    printf("  --index     <dir>      indexfile directory (root path)\n");
    printf("  --namespace <name>     which namespace to compact\n");
    printf("  --help                 print this message\n");

    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int option_index = 0;
    compaction_t settings = {
        .datapath = NULL,
        .indexpath = NULL,
        .namespace = NULL
    };

    while(1) {
        // int i = getopt_long_only(argc, argv, "d:i:l:p:vxh", long_options, &option_index);
        int i = getopt_long_only(argc, argv, "", long_options, &option_index);

        if(i == -1)
            break;

        switch(i) {
            case 'd':
                settings.datapath = optarg;
                break;

            case 'i':
                settings.indexpath = optarg;
                break;

            case 'n':
                settings.namespace = optarg;
                break;

            case 'h':
                usage();
                break;

            case '?':
            default:
               exit(EXIT_FAILURE);
        }
    }

    if(!settings.indexpath || !settings.datapath) {
        fprintf(stderr, "[-] missing index or data directory\n");
        usage();
    }

    if(!settings.namespace) {
        fprintf(stderr, "[-] missing namespace, you need to specify a namespace\n");
        usage();
    }

    printf("[+] zdb compacting tool\n");
    printf("[+] index root directory: %s\n", settings.indexpath);
    printf("[+] data root directory : %s\n", settings.datapath);
    printf("[+] namespace target    : %s\n", settings.namespace);

    if(validity_check(&settings))
        exit(EXIT_FAILURE);

    return namespace_compaction(&settings);
}
