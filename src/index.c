#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>
#include <limits.h>
#include <x86intrin.h>
#include <errno.h>
#include "zerodb.h"
#include "index.h"
#include "data.h"

// maximum allowed branch in memory
//
// this settings is mainly the most important to
// determine the keys lookup time
//
// the more bits you allows here, the more buckets
// can be used for lookup without collision
//
// the index works like a hash-table and uses crc32 'hash'
// algorithm, the result of the crc32 is used to point to
// the bucket, but using a full 32-bits hashlist would
// consume more than (2^32 * 8) GB of memory (on 64-bits)
//
// the default settings sets this to 24 bits, which allows
// 16 millions direct entries, collisions uses linked-list
//
// if you change this settings, please adapt
// the mask used on 'index_key_hash' function
// to avoid any overflow (you need to mask the value with
// same amount of bits)
#define BUCKET_BRANCHES (1 << 24)

// main global root index state
static index_root_t *rootindex = NULL;

// allows to works on readonly index (no write allowed)
static int index_status = INDEX_HEALTHY;

// wrap (mostly) all write operation on indexfile
// it's easier to keep a single logic with error handling
// related to write check
static int index_write(int fd, void *buffer, size_t length) {
    ssize_t response;

    if((response = write(fd, buffer, length)) < 0) {
        warnp("index write");
        return 0;
    }

    if(response != (ssize_t) length) {
        fprintf(stderr, "[-] index write: partial write\n");
        return 0;
    }

    return 1;
}


//
// index branch
// this implementation use a lazy load of branches
// this allows us to use lot of branch (BUCKET_BRANCHES in this case)
// without consuming all the memory if we don't need it
//
index_branch_t *index_branch_init(uint32_t branchid) {
    // debug("[+] initializing branch id 0x%x\n", branchid);

    rootindex->branches[branchid] = malloc(sizeof(index_branch_t));
    index_branch_t *branch = rootindex->branches[branchid];

    branch->length = 0;
    branch->last = NULL;
    branch->list = NULL;

    return branch;
}

void index_branch_free(uint32_t branchid) {
    // this branch was not allocated
    if(!rootindex->branches[branchid])
        return;

    index_entry_t *entry = rootindex->branches[branchid]->list;
    index_entry_t *next = NULL;

    // deleting branch content by
    // iterate over the linked-list
    for(; entry; entry = next) {
        next = entry->next;
        free(entry);
    }

    // deleting branch
    free(rootindex->branches[branchid]);
}

// returns branch from rootindex, if branch is not allocated yet, returns NULL
// useful for any read on the index in memory
index_branch_t *index_branch_get(uint32_t branchid) {
    return rootindex->branches[branchid];
}

// returns branch from rootindex, if branch doesn't exists, it will be allocated
// (useful for any write in the index in memory)
index_branch_t *index_branch_get_allocate(uint32_t branchid) {
    if(!rootindex->branches[branchid])
        return index_branch_init(branchid);

    debug("[+] branch exists: %lu entries\n", rootindex->branches[branchid]->length);
    return rootindex->branches[branchid];
}

// append an entry (item) to the memory list
// since we use a linked-list, the logic of appending
// only occures here
index_entry_t *index_branch_append(uint32_t branchid, index_entry_t *entry) {
    index_branch_t *branch;

    // grabbing the branch
    branch = index_branch_get_allocate(branchid);
    branch->length += 1;

    // adding this item and pointing previous last one
    // to this new one
    if(!branch->list)
        branch->list = entry;

    if(branch->last)
        branch->last->next = entry;

    branch->last = entry;
    entry->next = NULL;

    return entry;
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

static inline void index_dump_entry(index_entry_t *entry) {
    printf("[+] key %.*s: ", entry->idlength, entry->id);
    printf("offset %" PRIu64 ", length: %" PRIu64 "\n", entry->offset, entry->length);
}

// dumps the current index load
// fulldump flags enable printing each entry
static void index_dump(int fulldump) {
    size_t datasize = 0;
    size_t entries = 0;
    size_t indexsize = 0;
    size_t branches = 0;

    printf("[+] verifyfing index populated\n");

    if(fulldump)
        printf("[+] ===========================\n");

    // iterating over each buckets
    for(int b = 0; b < BUCKET_BRANCHES; b++) {
        index_branch_t *branch = index_branch_get(b);

        // skipping empty branch
        if(!branch)
            continue;

        branches += 1;
        index_entry_t *entry = branch->list;

        // iterating over the linked-list
        for(; entry; entry = entry->next) {
            if(fulldump)
                index_dump_entry(entry);

            indexsize += sizeof(index_entry_t) + entry->idlength;
            datasize += entry->length;

            entries += 1;
        }
    }

    if(fulldump) {
        if(entries == 0)
            printf("[+] index is empty\n");

        printf("[+] ===========================\n");
    }

    verbose("[+] index load: %lu entries\n", entries);
    verbose("[+] index uses: %lu branches\n", branches);

    verbose("[+] datasize expected: %.2f MB (%lu bytes)\n", (datasize / (1024.0 * 1024)), datasize);
    verbose("[+] index raw usage: %.2f KB (%lu bytes)\n", (indexsize / 1024.0), indexsize);

    // overhead contains:
    // - the buffer allocated to hold each (futur) branches pointer
    // - the branch struct itself for each branches
    size_t overhead = (BUCKET_BRANCHES * sizeof(index_branch_t **)) +
                      (branches * sizeof(index_branch_t));

    verbose("[+] memory overhead: %.2f KB (%lu bytes)\n", (overhead / 1024.0), overhead);
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

    if(!index_write(fd, &header, sizeof(index_t)))
        diep("index_initialize: write");

    return header;
}

static int index_try_rootindex(index_root_t *root) {
    // try to open the index file with create flag
    if((root->indexfd = open(root->indexfile, O_CREAT | O_RDWR, 0600)) < 0) {
        // okay it looks like we can't open this file
        // the only case we support is if the filesystem is in
        // read only, otherwise we just crash, this should not happen
        if(errno != EROFS)
            diep(root->indexfile);

        debug("[-] warning: read-only index filesystem\n");

        // okay, it looks like the index filesystem is in readonly
        // this can happen by choice or because the disk is unstable
        // and the system remounted-it in readonly, this won't stop
        // us to read it if we can, we won't change it
        if((root->indexfd = open(root->indexfile, O_RDONLY, 0600)) < 0) {
            // it looks like we can't open it, even in readonly
            // we need to keep in mind that the index file we requests
            // may not exists (we can then silently ignore this, we reached
            // the last index file found)
            if(errno == ENOENT)
                return 0;

            // if we are here, we can't read the indexfile for another reason
            // this is not supported, let's crash
            diep(root->indexfile);
        }

        // we keep track that we are on a readonly filesystem
        // we can't live with it, but with restriction
        index_status |= INDEX_READ_ONLY;
    }

    return 1;
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
    ssize_t length;

    verbose("[+] loading index file: %s\n", root->indexfile);

    if(!index_try_rootindex(root))
        return 0;

    if((length = read(root->indexfd, &header, sizeof(index_t))) != sizeof(index_t)) {
        if(length < 0) {
            // read failed, probably caused by a system error
            // this is probably an unrecoverable issue, let's skip this
            // index file (this could break consistancy)
            index_status |= INDEX_DEGRADED;
            return 1;
        }

        if(length > 0) {
            // we read something, but not the expected header, at least
            // not this amount of data, which is a completly undefined behavior
            // let's just stopping here
            fprintf(stderr, "[-] index: header corrupted or incomplete\n");
            fprintf(stderr, "[-] index: expected %lu bytes, %ld read\n", sizeof(index_t), length);
            exit(EXIT_FAILURE);
        }

        // we could not read anything, which basicly means that
        // the file is empty, we probably just created it
        //
        // if the current indexid is not zero, this is probablu
        // a new file not expected, otherwise if index is zero,
        // this is the initial index file we need to create
        if(root->indexid > 0) {
            verbose("[+] discarding index file\n");
            close(root->indexfd);
            return 0;
        }

        // if we are here, it's the first index file found
        // and we are in read-only mode, we can't write on the index
        // and it's empty, there is no goal to do anything
        // let's crash
        if(index_status & INDEX_READ_ONLY) {
            fprintf(stderr, "[-] no index found and readonly filesystem\n");
            fprintf(stderr, "[-] cannot starts correctly\n");
            exit(EXIT_FAILURE);
        }

        printf("[+] creating empty index file\n");
        header = index_initialize(root->indexfd, root->indexid);
    }


    // re-writing the header, with updated data if the index is writable
    // if the file was just created, it's okay, we have a new struct ready
    if(!(index_status & INDEX_READ_ONLY)) {
        // updating index with latest opening state
        header.opened = time(NULL);
        lseek(root->indexfd, 0, SEEK_SET);

        if(!index_write(root->indexfd, &header, sizeof(index_t)))
            diep(root->indexfile);
    }

    char date[256];
    verbose("[+] index created at: %s\n", index_date(header.created, date, sizeof(date)));
    verbose("[+] index last open: %s\n", index_date(header.opened, date, sizeof(date)));

    printf("[+] populating index: %s\n", root->indexfile);

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
    ssize_t ahead;
    index_item_t *entry = NULL;

    while(read(root->indexfd, &idlength, sizeof(idlength)) == sizeof(idlength)) {
        // we have the length of the key
        ssize_t entrylength = sizeof(index_item_t) + idlength;
        if(!(entry = realloc(entry, entrylength)))
            diep("realloc");

        // rollback the 1 byte read for the id length
        lseek(root->indexfd, -1, SEEK_CUR);

        if((ahead = read(root->indexfd, entry, entrylength)) != entrylength) {
            fprintf(stderr, "[-] index: invalid read during populate, skipping\n");
            fprintf(stderr, "[-] index: %lu bytes expected, %lu bytes read\n", entrylength, ahead);
            continue;
        }

        // insert this entry like it was inserted by a user
        // this allows us to keep a generic way of inserting data and keeping a
        // single point of logic when adding data (logic for overwrite, resize bucket, ...)
        index_entry_insert_memory(entry->id, entry->idlength, entry->offset, entry->length, entry->flags);
    }

    free(entry);

    close(root->indexfd);

    // if length is greater than 0, the index was existing
    // if length is 0, index just has been created
    return length;
}

// set global filename based on the index id
static void index_set_id(index_root_t *root) {
    sprintf(root->indexfile, "%s/rkv-index-%05u", root->indexdir, root->indexid);
}

// open the current filename set on the global struct
static void index_open_final(index_root_t *root) {
    int flags = O_CREAT | O_RDWR | O_APPEND;

    if(index_status & INDEX_READ_ONLY)
        flags = O_RDONLY;

    if((root->indexfd = open(root->indexfile, flags, 0600)) < 0) {
        warnp(root->indexfile);
        fprintf(stderr, "[-] could not open index file\n");
        return;
    }

    printf("[+] active index file: %s\n", root->indexfile);
}

// load all the index found
// if no index files exists, we create the original one
static void index_load(index_root_t *root) {
    for(root->indexid = 0; root->indexid < 65535; root->indexid++) {
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

    if(index_status & INDEX_READ_ONLY) {
        printf("[-] ========================================\n");
        printf("[-] WARNING: running in read-only mode\n");
        printf("[-] WARNING: index filesystem is not writable\n");
        printf("[-] ========================================\n");
    }

    if(index_status & INDEX_DEGRADED) {
        printf("[-] ========================================\n");
        printf("[-] WARNING: index degraded (read errors)\n");
        printf("[-] ========================================\n");
    }

    // opening the real active index file in append mode
    index_open_final(root);
}

// jumping to the next index id file, this needs to be sync'd with
// data file, we only do this when datafile changes basicly, this is
// triggered by a datafile too big event
size_t index_jump_next() {
    verbose("[+] jumping to the next index file\n");

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
    uint64_t *input = (uint64_t *) id;
    uint32_t hash = 0;
    ssize_t i = 0;

    for(i = 0; i < idlength - 8; i += 8)
        hash = _mm_crc32_u64(hash, *input++);

    for(; i < idlength; i++)
        hash = _mm_crc32_u8(hash, id[i]);

    return hash & 0xffffff;
}

// main look-up function, used to get an entry from the memory index
index_entry_t *index_entry_get(unsigned char *id, uint8_t idlength) {
    uint32_t branchkey = index_key_hash(id, idlength);
    index_branch_t *branch = index_branch_get(branchkey);
    index_entry_t *entry;

    // branch not exists
    if(!branch)
        return NULL;

    for(entry = branch->list; entry; entry = entry->next) {
        if(entry->idlength != idlength)
            continue;

        if(memcmp(entry->id, id, idlength) == 0)
            return entry;
    }

    return NULL;
}

// insert a key, only in memory, no disk is touched
// this function should be called externaly only when populating something
// if we need to add something new on the index, we should write it on disk
index_entry_t *index_entry_insert_memory(unsigned char *id, uint8_t idlength, size_t offset, size_t length, uint8_t flags) {
    index_entry_t *exists = NULL;

    // item already exists
    if((exists = index_entry_get(id, idlength))) {
        debug("[+] key already exists, overwriting\n");

        // re-use existing entry
        exists->length = length;
        exists->offset = offset;
        exists->flags = flags;

        return exists;
    }

    // calloc will ensure any unset fields (eg: flags) to zero
    index_entry_t *entry = calloc(sizeof(index_entry_t) + idlength, 1);

    memcpy(entry->id, id, idlength);
    entry->idlength = idlength;
    entry->offset = offset;
    entry->length = length;
    entry->dataid = rootindex->indexid;

    uint32_t branchkey = index_key_hash(id, idlength);

    // commit entry into memory
    index_branch_append(branchkey, entry);

    return entry;
}

// this will be a global item we will allocate only once, to avoid
// useless reallocation
// this item will be used to move from an index_entry_t (disk) to index_item_t (memory)
static index_item_t *transition = NULL;

// main function to insert anything on the index, in memory and on the disk
// perform at first a memory insertion then disk writing
//
// if the key already exists, the in memory version will be updated
// and the on-disk version is appended anyway, when reloading the index
// we call the same sets of function which overwrite existing key, we
// will always have the last version in memory
index_entry_t *index_entry_insert(unsigned char *id, uint8_t idlength, size_t offset, size_t length) {
    index_entry_t *entry = NULL;

    if(!(entry = index_entry_insert_memory(id, idlength, offset, length, 0)))
        return NULL;

    size_t entrylength = sizeof(index_item_t) + entry->idlength;

    memcpy(transition->id, entry->id, entry->idlength);
    transition->idlength = entry->idlength;
    transition->offset = entry->offset;
    transition->length = entry->length;
    transition->flags = entry->flags;
    transition->dataid = entry->dataid;

    if(!index_write(rootindex->indexfd, transition, entrylength)) {
        fprintf(stderr, "[-] index: cannot write index entry on disk\n");

        // it's easier to flag the entry as deleted than
        // removing it from the list
        entry->flags |= INDEX_ENTRY_DELETED;

        return NULL;
    }

    return entry;
}

index_entry_t *index_entry_delete(unsigned char *id, uint8_t idlength) {
    index_entry_t *entry = index_entry_get(id, idlength);

    if(!entry) {
        verbose("[-] key not found\n");
        return NULL;
    }

    if(entry->flags & INDEX_ENTRY_DELETED) {
        verbose("[-] key already deleted\n");
        return NULL;
    }

    // mark entry as deleted
    entry->flags |= INDEX_ENTRY_DELETED;

    // write flagged deleted entry on index file
    size_t entrylength = sizeof(index_item_t) + entry->idlength;

    memcpy(transition->id, entry->id, entry->idlength);
    transition->idlength = entry->idlength;
    transition->offset = entry->offset;
    transition->length = entry->length;
    transition->flags = entry->flags;
    transition->dataid = entry->dataid;

    if(!index_write(rootindex->indexfd, transition, entrylength))
        return NULL;

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
    free(transition);
}

// create an index and load files
uint16_t index_init(char *indexpath, int dump) {
    index_root_t *lroot = malloc(sizeof(index_root_t));

    debug("[+] initializing index (%d lazy branches)\n", BUCKET_BRANCHES);

    lroot->branches = (index_branch_t **) calloc(sizeof(index_branch_t *), BUCKET_BRANCHES);

    lroot->indexdir = indexpath;
    lroot->indexid = 0;
    lroot->indexfile = malloc(sizeof(char) * (PATH_MAX + 1));
    lroot->nextentry = 0;

    // commit variable
    rootindex = lroot;

    // allocating transition variable, 256 is the key limit size
    transition = malloc(sizeof(index_item_t) + 256);

    index_load(lroot);
    index_dump(dump);

    return lroot->indexid;
}

void index_emergency() {
    fsync(rootindex->indexfd);
}
