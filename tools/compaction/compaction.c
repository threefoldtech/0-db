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

typedef struct compaction_t {
    char *datapath;
    char *indexpath;
    char *namespace;

} compaction_t;

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

static char *index_date(uint32_t epoch, char *target, size_t length) {
    struct tm *timeval;
    time_t unixtime;

    unixtime = epoch;

    timeval = localtime(&unixtime);
    strftime(target, length, "%F %T", timeval);

    return target;
}

int index_dump(int fd) {
    index_t header;
    char entrydate[64];

    // first step, let's validate the header
    if(read(fd, &header, sizeof(index_t)) != (size_t) sizeof(index_t)) {
        fprintf(stderr, "[-] cannot read index header\n");
        return 1;
    }

    if(memcmp(header.magic, "IDX0", 4) != 0) {
        fprintf(stderr, "[-] index header magic mismatch\n");
        return 1;
    }

    if(header.version != ZDB_IDXFILE_VERSION) {
        fprintf(stderr, "[-] index version mismatch (%d <> %d)\n", header.version, ZDB_IDXFILE_VERSION);
        return 1;
    }

    printf("[+] index header seems correct\n");

    // now it's time to read each entries
    // each time, one entry starts by the entry-header
    // then entry payload.
    // the entry headers starts with the amount of bytes
    // of the key, which is needed to read the full header
    uint8_t idlength;
    index_item_t *entry = NULL;
    size_t entrycount = 0;

    while(read(fd, &idlength, sizeof(idlength)) == sizeof(idlength)) {
        // we have the length of the key
        ssize_t entrylength = sizeof(index_item_t) + idlength;
        if(!(entry = realloc(entry, entrylength)))
            diep("realloc");

        // rollback the 1 byte read for the id length
        off_t curoff = lseek(fd, -1, SEEK_CUR);

        if(read(fd, entry, entrylength) != entrylength)
            diep("index header read failed");

        entrycount += 1;

        index_date(entry->timestamp, entrydate, sizeof(entrydate));

        printf("[+] index entry: %lu, offset: %lu\n", entrycount, curoff);
        printf("[+]   id length  : %d\n", entry->idlength);
        printf("[+]   data length: %" PRIu64 "\n", entry->length);
        printf("[+]   data offset: %" PRIu64 "\n", entry->offset);
        printf("[+]   data fileid: %u\n", entry->dataid);
        printf("[+]   entry flags: 0x%X\n", entry->flags);
        printf("[+]   entry date : %s\n", entrydate);
        printf("[+]   entry key  : ");
        hexdump(entry->id, entry->idlength);
        printf("\n");
    }

    printf("[+] ---------------------------\n");
    printf("[+] all done, entry found: %lu\n", entrycount);

    free(entry);

    return 0;
}

int namespace_compaction(compaction_t *compaction) {
    settings_t zdbsettings = {
        .datapath = compaction->datapath,
        .indexpath = compaction->indexpath,
    };

    // allocate namespaces root object
    ns_root_t *nsroot = namespaces_allocate(&zdbsettings);

    // load our single namespace
    namespace_t *namespace = namespace_load_light(nsroot, compaction->namespace);

    return 0;
}

int file_check(char *target) {
    struct stat sb;

    if(stat(target, &sb) != 0)
        diep(target);

    if(!S_ISREG(sb.st_mode))
        return 1;

    return 0;
}

int directory_check(char *target) {
    struct stat sb;

    if(stat(target, &sb) != 0)
        diep(target);

    if(!S_ISDIR(sb.st_mode))
        return 1;

    return 0;
}

int validity_check(compaction_t *compaction) {
    char filename[256];
    char buffer[8192];
    int fd;

    // preliminary check
    // does index path is a directory
    if(directory_check(compaction->indexpath)) {
        fprintf(stderr, "[-] %s: target is not a directory\n", compaction->indexpath);
        return 1;
    }

    // does the namespace index directory exists
    snprintf(filename, sizeof(filename), "%s/%s", compaction->indexpath, compaction->namespace);
    if(directory_check(filename)) {
        fprintf(stderr, "[-] %s: target is not a directory\n", filename);
        return 1;
    }

    // does the data directory exists
    if(directory_check(compaction->datapath)) {
        fprintf(stderr, "[-] %s: target is not a directory\n", compaction->datapath);
        return 1;
    }

    // does data namespace directory exists
    snprintf(filename, sizeof(filename), "%s/%s", compaction->datapath, compaction->namespace);
    if(directory_check(filename)) {
        fprintf(stderr, "[-] %s: target is not a directory\n", filename);
        return 1;
    }

    // zdb validity check
    snprintf(filename, sizeof(filename), "%s/%s/zdb-namespace", compaction->indexpath, compaction->namespace);
    if(file_check(filename)) {
        fprintf(stderr, "[-] %s: invalid namespace descriptor\n", filename);;
        return 1;
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
