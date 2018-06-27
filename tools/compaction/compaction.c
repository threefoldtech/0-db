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
#include "branches.h"

static struct option long_options[] = {
    {"data",       required_argument, 0, 'd'},
    {"target",     required_argument, 0, 't'},
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

void dies(char *str) {
    fprintf(stderr, "[-] %s\n", str);
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

index_entry_t *compaction_handle_entry(index_root_t *index, data_entry_header_t *entry, char *id, datamap_t *datamap) {
    index_entry_t *idxentry = NULL;

    if((idxentry = index_entry_get(index, id, entry->idlength))) {
        printf("[+] entry found, already exists, updating\n");

        // FIXME: flag previous offset to be deleted !!

        idxentry->offset = datamap->length;
        idxentry->dataid = datamap->fileid;

        return idxentry;
    }

    if(!(idxentry = malloc(sizeof(index_entry_t) + entry->idlength)))
        diep("index entry malloc");

    // copy id and stuff
    memcpy(idxentry->id, id, entry->idlength);
    idxentry->idlength = entry->idlength;
    idxentry->dataid = datamap->fileid;
    idxentry->offset = datamap->length; // offset is object id in datamap

    uint32_t keyhash = index_key_hash(idxentry->id, idxentry->idlength);
    index_branch_append(index->branches, keyhash, idxentry);

    return idxentry;
}

size_t compaction_data_load(int fd, index_root_t *index, datamap_t *datamap) {
    data_header_t header;

    // reading static header
    if(read(fd, &header, sizeof(header)) != (ssize_t) sizeof(header))
        diep("header read");

    if(memcmp(header.magic, "DAT0", 4))
        dies("magic header mismatch, invalid datafile");

    if(header.version != ZDB_DATAFILE_VERSION)
        dies("wrong datafile version");

    // reading each entries
    data_entry_header_t entry;
    off_t offset = lseek(fd, 0, SEEK_CUR);
    char idbuffer[MAX_KEY_LENGTH];

    while(read(fd, &entry, sizeof(entry)) == (ssize_t) sizeof(entry)) {
        if(datamap->length + 1 > datamap->allocated) {
            size_t allocstep = 8192;
            size_t allocsize = sizeof(datamap_entry_t) * (datamap->allocated + allocstep);
            printf("[+] growing up datamap list to: %.2f KB\n", KB(allocsize));

            if(!(datamap->entries = realloc(datamap->entries, allocsize)))
                diep("datamap entries realloc");

            datamap->allocated += allocstep;
        }

        if(read(fd, idbuffer, entry.idlength) != (ssize_t) entry.idlength)
            diep("id read");

        // fill entry in linear map per fileid
        // add id in memory (hash) point to map
        //   if exists, set this map to discard
        //   update pointer to new entry
        //
        // when done, reading linear map, rewrite (or not) this source
        // offset/payload to target

        // fillin this entry and keeping it by default
        datamap->entries[datamap->length].offset = offset;
        datamap->entries[datamap->length].keep = !(entry.flags & DATA_ENTRY_DELETED);

        // add or update entry into id hash
        index_entry_t *idxentry;
        idxentry = compaction_handle_entry(index, &entry, idbuffer, datamap);

        // dump statistics
        printf("[+] entry: offset: %lu, keep: %d, id: ", offset, datamap->entries[datamap->length].keep);
        hexdump(idxentry->id, idxentry->idlength);
        printf("\n[+] entry: id: %u bytes, payload size: %u bytes\n", entry.idlength, entry.datalength);

        // computing next offset
        offset = lseek(fd, entry.datalength, SEEK_CUR);
        datamap->length += 1;
    }

    printf("[+] entries read: %lu\n", datamap->length);

    return datamap->length;
}

int namespace_compaction(compaction_t *compaction) {
    datamap_t **filesmap = NULL;
    char filename[512];
    int fd;

    settings_t zdbsettings = {
        .datapath = compaction->datapath,
        // .indexpath = compaction->indexpath,
    };

    // allocate namespaces root object
    ns_root_t *nsroot = namespaces_allocate(&zdbsettings);

    // load our single namespace
    namespace_t *namespace = namespace_load_light(nsroot, compaction->namespace);
    if(!(namespace->index = calloc(sizeof(index_root_t), 1)))
        diep("index calloc");

    // allocate a buckets branches
    namespace->index->branches = index_buckets_init();

    uint64_t maxfiles = (1 << (sizeof(((data_root_t *) 0)->dataid) * 8));
    size_t entries = 0;

    for(size_t fileid = 0; fileid < maxfiles; fileid++) {
        snprintf(filename, sizeof(filename), "%s/zdb-data-%05lu", namespace->datapath, fileid);
        printf("[+] opening file: %s\n", filename);

        printf("[+] growing up files map\n");
        if(!(filesmap = realloc(filesmap, sizeof(datamap_t) * (fileid + 1))))
            diep("datamap realloc");

        if(!(filesmap[fileid] = calloc(sizeof(datamap_t), 1)))
            diep("calloc");

        filesmap[fileid]->fileid = fileid;

        if((fd = open(filename, O_RDONLY)) < 0) {
            warnp(filename);
            printf("[+] all datafile loaded\n");
            break;
        }

        entries += compaction_data_load(fd, namespace->index, filesmap[fileid]);
        close(fd);
    }

    // compute memory usage
    // and index status (amount of keys can be less than
    // amount of entries, since overwrite and deleted keys are not counted)
    size_t branches = 0;
    size_t effective = 0;

    for(uint32_t b = 0; b < buckets_branches; b++) {
        index_branch_t *branch = index_branch_get(namespace->index->branches, b);

        if(!branch)
            continue;

        branches += 1;

        index_entry_t *entry = branch->list;

        for(; entry; entry = entry->next) {
            // do not count deleted entry
            if(!(entry->flags & INDEX_ENTRY_DELETED))
                effective += 1;
        }
    }

    printf("[+] data: load completed, %lu entries loaded\n", entries);
    printf("[+] index: %lu branches used, for %lu entries\n", branches, effective);

    return 0;
}

void usage() {
    printf("Compaction tool arguments:\n\n");

    printf("  --data      <dir>      datafile (input) root directory\n");
    printf("  --target    <dir>      datafile (output) root directory \n");
    printf("  --namespace <name>     which namespace to compact\n");
    printf("  --help                 print this message\n");

    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int option_index = 0;
    compaction_t settings = {
        .datapath = NULL,
        .targetpath = NULL,
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

            case 't':
                settings.targetpath = optarg;
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

    if(!settings.targetpath || !settings.datapath) {
        fprintf(stderr, "[-] missing source or destination data directory\n");
        usage();
    }

    if(!settings.namespace) {
        fprintf(stderr, "[-] missing namespace, you need to specify a namespace\n");
        usage();
    }

    printf("[+] zdb compacting tool\n");
    printf("[+] source root directory: %s\n", settings.datapath);
    printf("[+] target root directory : %s\n", settings.targetpath);
    printf("[+] namespace target    : %s\n", settings.namespace);

    if(validity_check(&settings))
        exit(EXIT_FAILURE);

    return namespace_compaction(&settings);
}
