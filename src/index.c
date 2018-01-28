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
#include "zerodb.h"
#include "index.h"
#include "data.h"

#define BUCKET_CHUNKS   32
#define BUCKET_BRANCHES (1 << 20)

static index_root_t *rootindex = NULL;

//
// index branch
// this implementation use a lazy load of branches
// this allows us to use lot of branch (BUCKET_BRANCH in this case) without
// consuming all the memory if we don't need it
//
// the current status allows us to store up to 3 bytes prefix directly
// after that, it's a simple unordered linked list
//
index_branch_t *index_branch_init(uint32_t branchid) {
    // printf("[+] initializing branch id 0x%x\n", branchid);

    rootindex->branches[branchid] = malloc(sizeof(index_branch_t));
    index_branch_t *branch = rootindex->branches[branchid];

    branch->length = BUCKET_CHUNKS;
    branch->next = 0;
    branch->entries = (index_entry_t **) malloc(sizeof(index_entry_t *) * branch->length);

    return branch;
}

void index_branch_free(uint32_t branchid) {
    if(!rootindex->branches[branchid])
        return;

    // deleting branch content
    for(size_t i = 0; i < rootindex->branches[branchid]->next; i++)
        free(rootindex->branches[branchid]->entries[i]);

    // deleting branch
    free(rootindex->branches[branchid]->entries);
    free(rootindex->branches[branchid]);
}

// returns branch from rootindex, if branch is not allocated yet, returns NULL
// useful for any read on the index in memory
index_branch_t *index_branch_get(uint32_t branchid) {
    return rootindex->branches[branchid];
}

// returns branch from rootindex, if branch doesn't exists, it will be allocated
// useful for any write in the index in memory
index_branch_t *index_branch_get_allocate(uint32_t branchid) {
    if(!rootindex->branches[branchid])
        return index_branch_init(branchid);

    return rootindex->branches[branchid];
}

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

// dumps the current index load
// fulldump flags enable printing each entry moreover
static void index_dump(int fulldump) {
    size_t datasize = 0;
    size_t entries = 0;
    size_t indexsize = 0;
    size_t branches = 0;
    size_t preallocated = 0;

    if(fulldump)
        printf("[+] ===========================\n");

    for(int b = 0; b < BUCKET_BRANCHES; b++) {
        index_branch_t *branch = index_branch_get(b);

        // skipping empty branch
        if(!branch)
            continue;

        branches += 1;
        preallocated += branch->length;

        for(size_t i = 0; i < branch->next; i++) {
            index_entry_t *entry = branch->entries[i];

            if(fulldump)
                printf("[+] key %.*s: offset %lu, length: %lu\n", entry->idlength, entry->id, entry->offset, entry->length);

            indexsize += sizeof(index_entry_t) + entry->idlength;
            datasize += entry->length;

            entries += 1;
        }
    }

    if(fulldump)
        printf("[+] ===========================\n");

    #if 0
    if(fulldump) {
        for(int b = 0; b < BUCKET_BRANCHES; b++) {
            index_branch_t *branch = index_branch_get(b);

            if(branch)
                printf("[+] branch 0x%x: %lu entries\n", b, branch->next);
        }

        printf("[+] ===========================\n");
    }
    #endif

    printf("[+] index load: %lu entries\n", entries);
    printf("[+] index uses: %lu branches\n", branches);

    printf("[+] datasize expected: %.2f MB (%lu bytes)\n", (datasize / (1024.0 * 1024)), datasize);
    printf("[+] index raw usage: %.2f KB (%lu bytes)\n", (indexsize / 1024.0), indexsize);

    // overhead contains:
    // - the buffer allocated to hold each (futur) branches pointer
    // - the branch struct itself for each branches
    // - each branch already allocated with their pre-registered buckets
    size_t overhead = (BUCKET_BRANCHES * sizeof(index_branch_t **)) +
                      (branches * sizeof(index_branch_t)) +
                      (preallocated * sizeof(index_entry_t *));

    printf("[+] memory overhead: %.2f KB (%lu bytes)\n", (overhead / 1024.0), overhead);
}

// initialize an index file
// this basicly create the header and write it
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
//
// the tricky part is, we need to create the initial index file
// if this one was not existing, but if the first one already exists
// this should not create any new index (when loading we will never create
// any new index until we don't have new data to add)
static size_t index_load_file(index_root_t *root) {
    index_t header;
    size_t length;

    printf("[+] loading index file: %s\n", root->indexfile);

    if((root->indexfd = open(root->indexfile, O_CREAT | O_RDWR, 0600)) < 0)
        diep(root->indexfile);

    if((length = read(root->indexfd, &header, sizeof(index_t))) != sizeof(index_t)) {
        if(length > 0) {
            // we read something, but not the expected header, at least
            // not this amount of data, which is a completly undefined behavior
            // let's just stopping here
            fprintf(stderr, "[-] index file corrupted or incomplete\n");
            exit(EXIT_FAILURE);
        }

        // we could not read anything, which basicly means that
        // the file is empty, we probably just created it
        //
        // if the current indexid is not zero, this is probablu
        // a new file not expected, otherwise if index is zero,
        // this is the initial index file we need to create
        if(root->indexid > 0) {
            printf("[+] discarding index file\n");
            close(root->indexfd);
            return 0;
        }

        printf("[+] creating empty index file\n");

        // creating new index
        header = index_initialize(root->indexfd, root->indexid);
    }

    // updating index with this opening state
    header.opened = time(NULL);
    lseek(root->indexfd, 0, SEEK_SET);

    // re-writing the header, with updated data
    // if the file was just created, it's okay, we have a new struct ready
    if(write(root->indexfd, &header, sizeof(index_t)) != sizeof(index_t))
        diep(root->indexfile);

    char date[256];
    printf("[+] index created at: %s\n", index_date(header.created, date, sizeof(date)));
    printf("[+] index last open: %s\n", index_date(header.opened, date, sizeof(date)));

    printf("[+] populating index...\n");

    // reading the index, populating memory
    //
    // here it's again a little bit dirty
    // we assume that key length is maximum 256 bytes, we stored this
    // size in a uint8_t, that means that for knowing each entry size, we
    // need to know the id length, which is the first field of the struct
    //
    // we read each time 1 byte, which will gives the id length, then
    // read sizeof(header) + length of the id which will be the full entry
    //
    // we always reuse the same entry object
    // we could use always a new object and keep the one read/allocated in memory
    // on the branches directly, but this break the genericity of the code below
    //
    // anyway, this is only at the boot-time, performance doesn't really matter
    uint8_t idlength;
    index_entry_t *entry = NULL;

    while(read(root->indexfd, &idlength, sizeof(idlength)) == sizeof(idlength)) {
        // we have the length of the key
        ssize_t entrylength = sizeof(index_entry_t) + idlength;
        if(!(entry = realloc(entry, entrylength)))
            diep("realloc");

        // rollback the 1 byte read for the id length
        lseek(root->indexfd, -1, SEEK_CUR);

        if(read(root->indexfd, entry, entrylength) != entrylength)
            diep("index read");

        // insert this entry like it was inserted by a user
        // this allows us to keep a generic way of inserting data and keeping a
        // single point of logic when adding data (logic for overwrite, resize bucket, ...)
        index_entry_insert_memory(entry->id, entry->idlength, entry->offset, entry->length);
    }

    free(entry);

    close(root->indexfd);

    // if length is greater than 0, the index was existing
    // if length is 0, index just has been created
    return length;
}

// set global filename based on the index id
static void index_set_id(index_root_t *root) {
    sprintf(root->indexfile, "%s/rkv-index-%04u", root->indexdir, root->indexid);
}

// open the current filename set on the global struct
static void index_open_final(index_root_t *root) {
    if((root->indexfd = open(root->indexfile, O_CREAT | O_RDWR | O_APPEND, 0600)) < 0)
        diep(root->indexfile);

    printf("[+] active index file: %s\n", root->indexfile);
}

// load all the index found
// if no index files exists, we create the original one
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

// jumping to the next index id file, this needs to be sync'd with
// data file, we only do this when datafile changes basicly, this is
// triggered by a datafile too big event
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
    // this is only used on sequential-id
    // it gives the next id
    return rootindex->nextentry++;
}

// perform the basic "hashing" (crc based) used to point to the expected branch
// we only keep partial amount of the result to now fill the memory too fast
static inline uint32_t index_key_hash(unsigned char *id, uint8_t idlength) {
    uint32_t key = 0xffffff;
    uint32_t mask;

    for(int i = 0; i != idlength; i++) {
        key = key ^ id[i];

        // perform a basic crc32
        for(int j = 7; j >= 0; j--) {
            mask = -(key & 1);
            key = (key >> 1) ^ (0xedb88320 & mask);
        }
    }

    return ~key & 0xfffff;
}

// main look-up function, used to get an entry from the memory index
index_entry_t *index_entry_get(unsigned char *id, uint8_t idlength) {
    uint32_t branchkey = index_key_hash(id, idlength);
    index_branch_t *branch = index_branch_get(branchkey);

    // branch not exists
    if(!branch)
        return NULL;

    for(size_t i = 0; i < branch->next; i++) {
        if(branch->entries[i]->idlength != idlength)
            continue;

        if(memcmp(branch->entries[i]->id, id, idlength) == 0)
            return branch->entries[i];
    }

    return NULL;
}

// insert a key, only in memory, no disk is touched
// this function should be called externaly only when populating something
// if we need to add something on the index, we should write it on disk
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

    uint32_t branchkey = index_key_hash(id, idlength);
    index_branch_t *branch = index_branch_get_allocate(branchkey);

    if(branch->next == branch->length) {
        // here is the branch bucket resize
        // we pre-allocate certain amount of bucket, but when
        // we filled the full bucket, we need to extends it
        printf("[+] buckets resize occures\n");
        branch->length = branch->length + BUCKET_CHUNKS;
        branch->entries = realloc(branch->entries, sizeof(index_entry_t *) * branch->length);
    }

    branch->entries[branch->next] = entry;
    branch->next += 1;

    return entry;
}

// main function to insert anything on the index, in memory and on the disk
// perform at first a memory insertion then disk writing
//
// if the key already exists, the in memory version will be updated
// and the on-disk version is appended anyway, when reloading the index
// we call the same sets of function which overwrite existing key, we
// will always have the last version in memory
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
    for(int b = 0; b < BUCKET_BRANCHES; b++)
        index_branch_free(b);

    // delete root object
    free(rootindex->branches);
    free(rootindex->indexfile);
    free(rootindex);
}

// create an index and load files
uint16_t index_init() {
    index_root_t *lroot = malloc(sizeof(index_root_t));

    printf("[+] initializing index (%d lazy branches)\n", BUCKET_BRANCHES);

    lroot->branches = (index_branch_t **) calloc(sizeof(index_branch_t *), BUCKET_BRANCHES);

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
