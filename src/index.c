#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include "rkv.h"
#include "index.h"
#include "data.h"

static index_root_t *rootindex = NULL;

static void index_dump(int fulldump) {
    size_t size = 0;
    size_t entries = 0;

    for(int b = 0; b < 256; b++) {
        index_branch_t *branch = rootindex->branches[b];

        for(size_t i = 0; i < branch->next; i++) {
            index_entry_t *entry = branch->entries[i];
            char *hex = sha256_hex((unsigned char *) entry->hash);

            if(fulldump)
                printf("[+] %s: offset %lu, length: %lu\n", hex, entry->offset, entry->length);

            size += entry->length;
            entries += 1;

            free(hex);
        }
    }

    size_t overhead = sizeof(data_header_t) * entries;

    printf("[+] index load: %lu entries\n", entries);
    printf("[+] datasize expected: %.2f MB (%lu bytes)\n", (size / (1024.0 * 1024)), size);
    printf("[+] dataindex overhead: %.2f KB (%lu bytes)\n", (overhead / 1024.0), overhead);
}


static void index_load(index_root_t *root) {
    index_t header;
    size_t length;
    index_entry_t entry;

    if((length = read(root->indexfd, &header, sizeof(index_t))) != sizeof(index_t)) {
        if(length > 0) {
            fprintf(stderr, "[-] index file corrupted or incomplete\n");
            exit(EXIT_FAILURE);
        }

        printf("[+] creating empty index file\n");

        // creating new index
        memcpy(header.magic, "IDX0", 4);
        header.version = 1;
        header.created = time(NULL);
    }

    // updating index
    header.opened = time(NULL);
    lseek(root->indexfd, 0, SEEK_SET);

    if(write(root->indexfd, &header, sizeof(index_t)) != sizeof(index_t))
        diep(root->indexfile);

    printf("[+] index created at: %u\n", header.created);
    printf("[+] index last open: %u\n", header.opened);

    // reading the index, populating memory
    while(read(root->indexfd, &entry, sizeof(index_entry_t)) == sizeof(index_entry_t))
        index_entry_insert_memory(entry.hash, entry.offset, entry.length);

    index_dump(1);
}

void index_init() {
    index_root_t *lroot = malloc(sizeof(index_root_t));

    printf("[+] initializing index\n");
    for(int i = 0; i < 256; i++) {
        lroot->branches[i] = malloc(sizeof(index_branch_t));
        index_branch_t *branch = lroot->branches[i];

        branch->length = 32;
        branch->next = 0;
        branch->entries = (index_entry_t **) malloc(sizeof(index_entry_t *) * branch->length);
    }

    lroot->indexfile = "/tmp/rkv-index";
    rootindex = lroot;

    // if((lroot->indexfd = open(lroot->indexfile, O_CREAT | O_SYNC | O_RDWR, 0600)) < 0)
    if((lroot->indexfd = open(lroot->indexfile, O_CREAT | O_RDWR, 0600)) < 0)
    // if((lroot->indexfd = open(lroot->indexfile, O_CREAT | O_DSYNC | O_RDWR, 0600)) < 0)
        diep(lroot->indexfile);

    index_load(lroot);
}

index_entry_t *index_entry_get(unsigned char *hash) {
    index_branch_t *branch = rootindex->branches[hash[0]];

    for(size_t i = 0; i < branch->next; i++) {
        if(memcmp(branch->entries[i]->hash, hash, HASHSIZE) == 0)
            return branch->entries[i];
    }

    return NULL;
}

index_entry_t *index_entry_insert_memory(unsigned char *hash, size_t offset, size_t length) {
    // item already exists
    if(index_entry_get(hash))
        return NULL;

    index_entry_t *entry = calloc(sizeof(index_entry_t), 1);

    memcpy(entry->hash, hash, HASHSIZE);
    entry->offset = offset;
    entry->length = length;

    // maybe resize
    index_branch_t *branch = rootindex->branches[hash[0]];

    if(branch->next == branch->length) {
        printf("[+] buckets resize occures\n");
        branch->length = branch->length + 32;
        branch->entries = realloc(branch->entries, sizeof(index_entry_t *) * branch->length);
    }

    branch->entries[branch->next] = entry;
    branch->next += 1;

    return entry;
}

index_entry_t *index_entry_insert(unsigned char *hash, size_t offset, size_t length) {
    index_entry_t *entry = NULL;

    if(!(entry = index_entry_insert_memory(hash, offset, length)))
        return NULL;

    if(write(rootindex->indexfd, entry, sizeof(index_entry_t)) != sizeof(index_entry_t))
        diep(rootindex->indexfile);

    return entry;
}

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
    free(rootindex);
}
