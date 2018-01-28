#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>
#include "rkv.h"
#include "index.h"
#include "data.h"

static index_root_t *rootindex = NULL;

//
// index initialized
//
static char *index_date(uint32_t epoch, char *target, size_t length) {
    struct tm *timeval;
    time_t unixtime;

    unixtime = epoch;

    timeval = localtime(&unixtime);
    strftime(target, length, "%F %T", timeval);

    return target;
}

static void index_dump(int fulldump) {
    size_t size = 0;
    size_t entries = 0;

    if(fulldump)
        printf("[+] ===========================\n");

    for(int b = 0; b < 256; b++) {
        index_branch_t *branch = rootindex->branches[b];

        for(size_t i = 0; i < branch->next; i++) {
            index_entry_t *entry = branch->entries[i];

            if(fulldump)
                printf("[+] key %.*s: offset %lu, length: %lu\n", entry->idlength, entry->id, entry->offset, entry->length);

            size += entry->length;
            entries += 1;
        }
    }

    if(fulldump)
        printf("[+] ===========================\n");

    size_t overhead = sizeof(data_header_t) * entries;

    printf("[+] index load: %lu entries\n", entries);
    printf("[+] datasize expected: %.2f MB (%lu bytes)\n", (size / (1024.0 * 1024)), size);
    printf("[+] dataindex overhead: %.2f KB (%lu bytes)\n", (overhead / 1024.0), overhead);
}

static index_t index_initialize(int fd, uint16_t indexid) {
    index_t header;

    memcpy(header.magic, "IDX0", 4);
    header.version = 1;
    header.created = time(NULL);
    header.fileid = indexid;
    header.opened = time(NULL);

    if(write(fd, &header, sizeof(index_t)) != sizeof(index_t))
        diep("index_initialize: write");

    return header;
}

// opening, reading then closing the index file
// if the index was created, 0 is returned
static size_t index_load_file(index_root_t *root) {
    index_t header;
    size_t length;

    printf("[+] loading index file: %s\n", root->indexfile);

    if((root->indexfd = open(root->indexfile, O_CREAT | O_RDWR, 0600)) < 0)
        diep(root->indexfile);

    if((length = read(root->indexfd, &header, sizeof(index_t))) != sizeof(index_t)) {
        if(length > 0) {
            fprintf(stderr, "[-] index file corrupted or incomplete\n");
            exit(EXIT_FAILURE);
        }

        // this is not the first file, it was not existing
        // no need to create or prepare it, we will discard it
        if(root->indexid > 0) {
            printf("[+] discarding index file\n");
            close(root->indexfd);
            return 0;
        }

        printf("[+] creating empty index file\n");

        // creating new index
        header = index_initialize(root->indexfd, root->indexid);
    }

    // updating index
    header.opened = time(NULL);
    lseek(root->indexfd, 0, SEEK_SET);

    if(write(root->indexfd, &header, sizeof(index_t)) != sizeof(index_t))
        diep(root->indexfile);

    char date[256];
    printf("[+] index created at: %s\n", index_date(header.created, date, sizeof(date)));
    printf("[+] index last open: %s\n", index_date(header.opened, date, sizeof(date)));

    // reading the index, populating memory
    uint8_t idlength;
    index_entry_t *entry = NULL;

    while(read(root->indexfd, &idlength, sizeof(idlength)) == sizeof(idlength)) {
        ssize_t entrylength = sizeof(index_entry_t) + idlength;
        if(!(entry = realloc(entry, entrylength)))
            diep("realloc");

        lseek(root->indexfd, -1, SEEK_CUR);

        if(read(root->indexfd, entry, entrylength) != entrylength)
            diep("index read");

        index_entry_insert_memory(entry->id, entry->idlength, entry->offset, entry->length);
    }

    free(entry);

    close(root->indexfd);

    // if length is greater than 0, the index was existing
    // if length is 0, index just has been created
    return length;
}

static void index_set_id(index_root_t *root) {
    sprintf(root->indexfile, "%s/rkv-index-%04u", root->indexdir, root->indexid);
}

static void index_open_final(index_root_t *root) {
    if((root->indexfd = open(root->indexfile, O_CREAT | O_RDWR | O_APPEND, 0600)) < 0)
        diep(root->indexfile);

    printf("[+] active index file: %s\n", root->indexfile);
}

static void index_load(index_root_t *root) {
    for(root->indexid = 0; root->indexid < 10000; root->indexid++) {
        index_set_id(root);

        if(index_load_file(root) == 0) {
            // if the index was not the first one
            // we created a new index, we need to remove it
            // and fallback to the previous one
            if(root->indexid > 0) {
                unlink(root->indexfile);
                root->indexid -= 1;
            }

            // writing the final filename
            index_set_id(root);
            break;
        }
    }

    // opening the real active index file in append mode
    index_open_final(root);
}

size_t index_jump_next() {
    printf("[+] jumping to the next index file\n");

    // closing current file descriptor
    close(rootindex->indexfd);

    // moving to the next file
    rootindex->indexid += 1;
    index_set_id(rootindex);

    index_open_final(rootindex);
    index_initialize(rootindex->indexfd, rootindex->indexid);

    return rootindex->indexid;
}

//
// index manipulation
//
uint64_t index_next_id() {
    return rootindex->nextentry++;
}

index_entry_t *index_entry_get(unsigned char *id, uint8_t idlength) {
    index_branch_t *branch = rootindex->branches[*id % 256];

    for(size_t i = 0; i < branch->next; i++) {
        if(branch->entries[i]->idlength != idlength)
            continue;

        if(memcmp(branch->entries[i]->id, id, idlength) == 0)
            return branch->entries[i];
    }

    return NULL;
}

index_entry_t *index_entry_insert_memory(unsigned char *id, uint8_t idlength, size_t offset, size_t length) {
    index_entry_t *exists = NULL;

    // item already exists
    if((exists = index_entry_get(id, idlength))) {
        // re-use existing entry
        exists->length = length;
        exists->offset = offset;

        return exists;
    }

    index_entry_t *entry = calloc(sizeof(index_entry_t) + idlength, 1);

    memcpy(entry->id, id, idlength);
    entry->idlength = idlength;
    entry->offset = offset;
    entry->length = length;
    entry->dataid = rootindex->indexid;

    // maybe resize
    index_branch_t *branch = rootindex->branches[entry->id[0] % 256];

    if(branch->next == branch->length) {
        printf("[+] buckets resize occures\n");
        branch->length = branch->length + 512;
        branch->entries = realloc(branch->entries, sizeof(index_entry_t *) * branch->length);
    }

    branch->entries[branch->next] = entry;
    branch->next += 1;

    return entry;
}

index_entry_t *index_entry_insert(unsigned char *id, uint8_t idlength, size_t offset, size_t length) {
    index_entry_t *entry = NULL;

    if(!(entry = index_entry_insert_memory(id, idlength, offset, length)))
        return NULL;

    size_t entrylength = sizeof(index_entry_t) + entry->idlength;

    if(write(rootindex->indexfd, entry, entrylength) != (ssize_t) entrylength)
        diep(rootindex->indexfile);

    return entry;
}

//
// index constructor and destructor
//

// clean all opened index related stuff
void index_destroy() {
    for(int b = 0; b < 256; b++) {
        index_branch_t *branch = rootindex->branches[b];

        // deleting branch content
        for(size_t i = 0; i < branch->next; i++)
            free(branch->entries[i]);

        // deleting branch
        free(rootindex->branches[b]->entries);
        free(rootindex->branches[b]);
    }

    // delete root object
    free(rootindex->indexfile);
    free(rootindex);
}

// create an index and load files
uint16_t index_init() {
    index_root_t *lroot = malloc(sizeof(index_root_t));

    printf("[+] initializing index\n");
    for(int i = 0; i < 256; i++) {
        lroot->branches[i] = malloc(sizeof(index_branch_t));
        index_branch_t *branch = lroot->branches[i];

        branch->length = 512;
        branch->next = 0;
        branch->entries = (index_entry_t **) malloc(sizeof(index_entry_t *) * branch->length);
    }

    lroot->indexdir = "/mnt/storage/tmp/rkv";
    lroot->indexid = 0;
    lroot->indexfile = malloc(sizeof(char) * (PATH_MAX + 1));
    lroot->nextentry = 0;

    // commit variable
    rootindex = lroot;

    index_load(lroot);
    index_dump(1);

    return lroot->indexid;
}

void index_emergency() {
    fsync(rootindex->indexfd);
}
