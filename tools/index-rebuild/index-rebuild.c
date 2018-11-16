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
#include "index_loader.h"
#include "data.h"
#include "namespace.h"
#include "index-rebuild.h"
#include "validity.h"

static struct option long_options[] = {
    {"data",       required_argument, 0, 'd'},
    {"index",      required_argument, 0, 'i'},
    {"namespace",  required_argument, 0, 'n'},
    {"template",   required_argument, 0, 't'},
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

buffer_t *buffer_new() {
    buffer_t *buffer;

    if(!(buffer = malloc(sizeof(buffer_t))))
        diep("buffer init malloc");

    // initial 1 MB buffer
    buffer->allocated = 1 * 1024 * 1024;

    // buffer is empty now
    buffer->length = 0;

    if(!(buffer->buffer = malloc(buffer->allocated)))
        diep("buffer payload malloc");

    buffer->writer = buffer->buffer;

    return buffer;
}

buffer_t *buffer_enlarge(buffer_t *buffer) {
    // grow up 1 MB
    buffer->allocated += 1 * 1024 * 1024;

    if(!(buffer->buffer = realloc(buffer->buffer, buffer->allocated)))
        diep("buffer realloc");

    buffer->writer = buffer->buffer + buffer->length;

    return buffer;
}

void buffer_free(buffer_t *buffer) {
    free(buffer->buffer);
    free(buffer);
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

int index_rebuild_commit(index_root_t *root, buffer_t *buffer, uint16_t fileid) {
    // initialize file and header
    root->indexid = fileid;
    index_set_id(root);
    index_open_final(root);
    index_initialize(root->indexfd, fileid, root);

    // write the whole buffer
    if(write(root->indexfd, buffer->buffer, buffer->length) != (ssize_t) buffer->length)
        diep("write");

    // cleaning
    close(root->indexfd);

    return 0;
}

size_t index_rebuild(int fd, buffer_t *buffer, uint16_t dataid) {
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
    char entryid[MAX_KEY_LENGTH];

    off_t offset = lseek(fd, 0, SEEK_CUR);
    size_t entries = 0;

    while(read(fd, &entry, sizeof(entry)) == (ssize_t) sizeof(entry)) {
        if(buffer->length + 256 > buffer->allocated)
            buffer_enlarge(buffer);

        if(read(fd, entryid, entry.idlength) != entry.idlength)
            diep("data id read");

        index_item_t *idxitem = (index_item_t *) buffer->writer;

        memcpy(idxitem->id, entryid, entry.idlength);
        idxitem->idlength = entry.idlength;
        idxitem->offset = offset;
        idxitem->length = entry.datalength;
        idxitem->flags = entry.flags;
        idxitem->dataid = dataid;
        idxitem->timestamp = entry.timestamp;

        // update buffer pointers
        buffer->length += sizeof(index_item_t) + entry.idlength;
        buffer->writer = buffer->buffer + buffer->length;

        printf("[+] entry: offset: %lu\n", offset);
        printf("[+] entry: id: %" PRIu8 " bytes, payload size: %" PRIu32 " bytes\n", idxitem->idlength, idxitem->length);

        // computing next offset
        offset = lseek(fd, entry.datalength, SEEK_CUR);
        entries += 1;
    }

    printf("[+] index size: %.2f KB\n", KB(buffer->length));
    printf("[+] entries read: %lu\n", entries);

    return entries;
}

index_root_t *index_init_wrap(namespace_t *namespace) {
    index_root_t *root;

    if(!(root = calloc(sizeof(index_root_t), 1)))
        diep("calloc");

    root->indexdir = namespace->indexpath;
    root->indexfile = malloc(sizeof(char) * (ZDB_PATH_MAX + 1));
    root->status = INDEX_NOT_LOADED | INDEX_HEALTHY;
    root->namespace = namespace;

    return root;
}

int namespace_index_rebuild(rebuild_t *compaction) {
    char filename[512];
    int fd;

    settings_t zdbsettings = {
        .datapath = compaction->datapath,
        .indexpath = compaction->indexpath,
    };

    // allocate namespaces root object
    ns_root_t *nsroot = namespaces_allocate(&zdbsettings);

    // load our single namespace
    // this will create the namespace descriptor file
    namespace_t *namespace = namespace_load_light(nsroot, compaction->namespace);
    uint64_t maxfiles = (1 << (sizeof(((data_root_t *) 0)->dataid) * 8));
    size_t entries = 0;
    buffer_t *idxbuf;

    namespace->index = index_init_wrap(namespace);

    // for each datafile found, we will create a new index file
    // this index will be created completely in memory, then written
    // on the disk when it's done, this will speed up a lot process
    // avoiding doing lot of few bytes write on the disk
    for(size_t fileid = 0; fileid < maxfiles; fileid++) {
        snprintf(filename, sizeof(filename), "%s/zdb-data-%05lu", namespace->datapath, fileid);
        printf("[+] opening file: %s\n", filename);

        if((fd = open(filename, O_RDONLY)) < 0) {
            warnp(filename);
            printf("[+] all datafile loaded\n");
            break;
        }

        idxbuf = buffer_new();
        entries += index_rebuild(fd, idxbuf, fileid);

        index_rebuild_commit(namespace->index, idxbuf, fileid);

        // freeing this buffer
        buffer_free(idxbuf);
        close(fd);
    }

    printf("[+] data: load completed, %lu entries loaded\n", entries);

    return 0;
}

void usage() {
    printf("Index rebuild tool arguments:\n\n");

    printf("  --data      <dir>      datafile directory (root path), input\n");
    printf("  --index     <dir>      indexfile directory (root path), output\n");
    printf("  --namespace <name>     which namespace to compact\n");
    printf("  --template  <file>     zdb-namespace source file (namespace settings)\n");
    printf("  --help                 print this message\n");

    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int option_index = 0;
    rebuild_t settings = {
        .datapath = NULL,
        .indexpath = NULL,
        .namespace = NULL,
        .template = NULL,
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

            case 't':
                settings.template = optarg;
                break;

            case 'h':
                usage();
                break;

            case '?':
            default:
                fprintf(stderr, "Unsupported option\n");
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

    printf("[+] zdb index rebuild tool\n");
    printf("[+] namespace target: %s\n", settings.namespace);
    printf("[+] index root directory (output): %s\n", settings.indexpath);
    printf("[+] data root directory  (input) : %s\n", settings.datapath);

    if(validity_check(&settings))
        exit(EXIT_FAILURE);

    return namespace_index_rebuild(&settings);
}
