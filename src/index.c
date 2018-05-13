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
#include <limits.h>
#include <x86intrin.h>
#include "zerodb.h"
#include "index.h"
#include "index_loader.h"
#include "index_branch.h"
#include "data.h"

// NOTE: there is no more a global variable for the index
//       since each namespace have their own index, now
//       we need to pass the index to each function

// force index to be sync'd with underlaying device
static inline int index_sync(index_root_t *root, int fd) {
    fsync(fd);
    root->lastsync = time(NULL);
    return 1;
}

// checking is some sync is forced
// there is two possibilities:
// - we set --sync option on runtime, and each write is sync forced
// - we set --synctime on runtime and after this amount of seconds
//   we force to sync the last write
static inline int index_sync_check(index_root_t *root, int fd) {
    if(root->sync)
        return index_sync(root, fd);

    if(!root->synctime)
        return 0;

    if((time(NULL) - root->lastsync) > root->synctime) {
        debug("[+] index: last sync expired, force sync\n");
        return index_sync(root, fd);
    }

    return 0;
}


// wrap (mostly) all write operation on indexfile
// it's easier to keep a single logic with error handling
// related to write check
int index_write(int fd, void *buffer, size_t length, index_root_t *root) {
    ssize_t response;

    if((response = write(fd, buffer, length)) < 0) {
        warnp("index write");
        return 0;
    }

    if(response != (ssize_t) length) {
        fprintf(stderr, "[-] index write: partial write\n");
        return 0;
    }

    index_sync_check(root, fd);

    return 1;
}


// set global filename based on the index id
void index_set_id(index_root_t *root) {
    sprintf(root->indexfile, "%s/zdb-index-%05u", root->indexdir, root->indexid);
}

// open the current filename set on the global struct
void index_open_final(index_root_t *root) {
    int flags = O_CREAT | O_RDWR | O_APPEND;

    if(root->status & INDEX_READ_ONLY)
        flags = O_RDONLY;

    if((root->indexfd = open(root->indexfile, flags, 0600)) < 0) {
        warnp(root->indexfile);
        fprintf(stderr, "[-] index: could not open index file\n");
        return;
    }

    printf("[+] active index file: %s\n", root->indexfile);
}

// jumping to the next index id file, this needs to be sync'd with
// data file, we only do this when datafile changes basicly, this is
// triggered by a datafile too big event
size_t index_jump_next(index_root_t *root) {
    verbose("[+] index: jumping to the next file\n");

    // closing current file descriptor
    close(root->indexfd);

    // moving to the next file
    root->indexid += 1;
    index_set_id(root);

    index_open_final(root);
    index_initialize(root->indexfd, root->indexid, root);

    return root->indexid;
}

//
// index manipulation
//
uint64_t index_next_id(index_root_t *root) {
    // this is only used on sequential-id
    // it gives the next id
    return root->nextentry;
}

// perform the basic "hashing" (crc based) used to point to the expected branch
// we only keep partial amount of the result to not fill the memory too fast
static inline uint32_t index_key_hash(unsigned char *id, uint8_t idlength) {
    uint64_t *input = (uint64_t *) id;
    uint32_t hash = 0;
    ssize_t i = 0;

    for(i = 0; i < idlength - 8; i += 8)
        hash = _mm_crc32_u64(hash, *input++);

    for(; i < idlength; i++)
        hash = _mm_crc32_u8(hash, id[i]);

    return hash & buckets_mask;
}

// main look-up function, used to get an entry from the memory index
index_entry_t *index_entry_get(index_root_t *root, unsigned char *id, uint8_t idlength) {
    uint32_t branchkey = index_key_hash(id, idlength);
    index_branch_t *branch = index_branch_get(root, branchkey);
    index_entry_t *entry;

    // branch not exists
    if(!branch)
        return NULL;

    for(entry = branch->list; entry; entry = entry->next) {
        if(entry->idlength != idlength)
            continue;

        if(entry->namespace != root->namespace)
            continue;

        if(memcmp(entry->id, id, idlength) == 0)
            return entry;
    }

    return NULL;
}

// insert a key, only in memory, no disk is touched
// this function should be called externaly only when populating something
// if we need to add something new on the index, we should write it on disk
index_entry_t *index_entry_insert_memory(index_root_t *root, unsigned char *id, uint8_t idlength, size_t offset, size_t length, uint8_t flags) {
    index_entry_t *exists = NULL;

    // item already exists
    if((exists = index_entry_get(root, id, idlength))) {
        debug("[+] index: key already exists, overwriting\n");

        // update statistics
        root->datasize -= exists->length;
        root->datasize += length;

        // re-use existing entry
        exists->length = length;
        exists->offset = offset;
        exists->flags = flags;
        exists->dataid = root->indexid;

        return exists;
    }

    // calloc will ensure any unset fields (eg: flags) are zero
    size_t entrysize = sizeof(index_entry_t) + idlength;
    index_entry_t *entry = calloc(entrysize, 1);

    memcpy(entry->id, id, idlength);
    entry->namespace = root->namespace;
    entry->idlength = idlength;
    entry->offset = offset;
    entry->length = length;
    entry->dataid = root->indexid;

    uint32_t branchkey = index_key_hash(id, idlength);

    // commit entry into memory
    index_branch_append(root, branchkey, entry);

    // update statistics
    root->entries += 1;
    root->datasize += length;
    root->indexsize += entrysize;

    // update next entry id
    root->nextentry += 1;

    return entry;
}

// this will be a global item we will allocate only once, to avoid
// useless reallocation
// this item will be used to move from an index_entry_t (disk) to index_item_t (memory)
index_item_t *index_transition = NULL;
index_entry_t *index_reusable_entry = NULL;

// main function to insert anything on the index, in memory and on the disk
// perform at first a memory insertion then disk writing
//
// if the key already exists, the in memory version will be updated
// and the on-disk version is appended anyway, when reloading the index
// we call the same sets of function which overwrite existing key, we
// will always have the last version in memory
index_entry_t *index_entry_insert(index_root_t *root, void *vid, uint8_t idlength, size_t offset, size_t length) {
    unsigned char *id = (unsigned char *) vid;
    index_entry_t *entry = NULL;

    if(!(entry = index_entry_insert_memory(root, id, idlength, offset, length, 0)))
        return NULL;

    size_t entrylength = sizeof(index_item_t) + entry->idlength;

    memcpy(index_transition->id, entry->id, entry->idlength);
    index_transition->idlength = entry->idlength;
    index_transition->offset = entry->offset;
    index_transition->length = entry->length;
    index_transition->flags = entry->flags;
    index_transition->dataid = entry->dataid;

    if(!index_write(root->indexfd, index_transition, entrylength, root)) {
        fprintf(stderr, "[-] index: cannot write index entry on disk\n");

        // it's easier to flag the entry as deleted than
        // removing it from the list
        entry->flags |= INDEX_ENTRY_DELETED;

        return NULL;
    }

    return entry;
}

index_entry_t *index_entry_delete(index_root_t *root, index_entry_t *entry) {
    /*
    index_entry_t *entry = index_entry_get(root, id, idlength);

    if(!entry) {
        verbose("[-] index: key not found\n");
        return NULL;
    }

    if(entry->flags & INDEX_ENTRY_DELETED) {
        verbose("[-] index: key already deleted\n");
        return NULL;
    }
    */

    // mark entry as deleted
    entry->flags |= INDEX_ENTRY_DELETED;

    // write flagged deleted entry on index file
    size_t entrylength = sizeof(index_item_t) + entry->idlength;

    memcpy(index_transition->id, entry->id, entry->idlength);
    index_transition->idlength = entry->idlength;
    index_transition->offset = entry->offset;
    index_transition->length = entry->length;
    index_transition->flags = entry->flags;
    index_transition->dataid = entry->dataid;

    if(!index_write(root->indexfd, index_transition, entrylength, root))
        return NULL;

    return entry;
}

// return the offset of the next entry which will be added
// this could be needed, for exemple in direct key mode,
// when the key depends of the offset itself
size_t index_next_offset(index_root_t *root) {
    return lseek(root->indexfd, 0, SEEK_END);
}


// return current fileid in use
uint16_t index_indexid(index_root_t *root) {
    return root->indexid;
}

// return 1 or 0 if index entry is deleted or not
int index_entry_is_deleted(index_entry_t *entry) {
    return (entry->flags & INDEX_ENTRY_DELETED);
}

// iterate over all entries in a single branch
// and remove if this entry is related to requested namespace
static inline size_t index_clean_namespace_branch(index_branch_t *branch, void *namespace) {
    index_entry_t *entry = branch->list;
    index_entry_t *previous = NULL;
    size_t deleted  = 0;

    while(entry) {
        if(entry->namespace != namespace) {
            // keeping this key, looking forward
            previous = entry;
            entry = entry->next;
            continue;
        }

        #ifndef RELEASE
        printf("[+] index: namespace cleaner: free: ");
        hexdump(entry->id, entry->idlength);
        printf("\n");
        #endif

        // okay, we need to remove this key
        index_entry_t *next = entry->next;
        index_entry_t *removed = index_branch_remove(branch, entry, previous);

        free(removed);
        deleted += 1;

        entry = next;
    }

    return deleted;
}

// remove specific namespace from the index
//
// we use a global index for everything, when removing a
// namespace, we walk over all the keys and remove keys matching
// to this namespace
int index_clean_namespace(index_root_t *root, void *namespace) {
    index_branch_t **branches = root->branches;
    size_t deleted = 0;

    if(!branches)
        return 0;

    debug("[+] index: starting namespace cleaner\n");

    for(uint32_t b = 0; b < buckets_branches; b++) {
        if(!branches[b])
            continue;

        deleted += index_clean_namespace_branch(branches[b], namespace);
    }

    debug("[+] index: namespace cleaner: %lu keys removed\n", deleted);

    return 0;
}

//
// index constructor and destructor
//
int index_emergency(index_root_t *root) {
    // skipping building index stage
    if(!root || (root->status & INDEX_NOT_LOADED))
        return 0;

    fsync(root->indexfd);
    return 1;
}
