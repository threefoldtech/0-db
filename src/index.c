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
#include "index_seq.h"
#include "index_branch.h"
#include "data.h"
#include "hook.h"

// NOTE: there is no more a global variable for the index
//       since each namespace have their own index, now
//       we need to pass the index to each function

// dump an index entry item
void index_item_header_dump(index_item_t *item) {
#ifdef RELEASE
    (void) item;
#else
    debug("[+] index: entry dump: id length  : %" PRIu8  "\n", item->idlength);
    debug("[+] index: entry dump: data offset: %" PRIu32 "\n", item->offset);
    debug("[+] index: entry dump: data length: %" PRIu32 "\n", item->length);
    debug("[+] index: entry dump: previous   : %" PRIX32 "\n", item->previous);
    debug("[+] index: entry dump: flags      : %" PRIu8  "\n", item->flags);
    debug("[+] index: entry dump: timestamp  : %" PRIu32 "\n", item->timestamp);
    debug("[+] index: entry dump: parent id  : %" PRIu16 "\n", item->parentid);
    debug("[+] index: entry dump: parent offs: %" PRIu32 "\n", item->parentoff);
#endif
}


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

static int index_read(int fd, void *buffer, size_t length) {
    ssize_t response;

    if((response = read(fd, buffer, length)) < 0) {
        warnp("index read");
        return 0;
    }

    if(response == 0) {
        fprintf(stderr, "[-] index read: eof reached\n");
        return 0;
    }

    if(response != (ssize_t) length) {
        fprintf(stderr, "[-] index read: partial read\n");
        return 0;
    }

    return 1;
}

// set global filename based on the index id
void index_set_id(index_root_t *root) {
    sprintf(root->indexfile, "%s/zdb-index-%05u", root->indexdir, root->indexid);
}

static int index_open_file_mode(index_root_t *root, uint16_t fileid, int mode) {
    char filename[512];
    int fd;

    sprintf(filename, "%s/zdb-index-%05u", root->indexdir, fileid);
    debug("[+] index: opening file: %s (ro: %s)\n", filename, (mode & O_RDONLY) ? "yes" : "no");

    if((fd = open(filename, mode)) < 0) {
        verbosep("index_open_file_mode", filename);
        return -1;
    }

    return fd;
}

static int index_open_file(index_root_t *root, int fileid) {
    return index_open_file_mode(root, fileid, O_RDONLY);
}

static int index_open_file_rw(index_root_t *root, int fileid) {
    return index_open_file_mode(root, fileid, O_RDWR);
}

// main function to call when you need to deal with multiple index id
// this function takes care to open the right file id:
//  - if you want the current opened file id, you have thid fd
//  - if the file is not opened yet, you'll receive a new fd
// you need to call the index_release_dataid to be consistant about cleaning this
// file open, if a new one was opened
//
// if the index id could not be opened, -1 is returned
inline int index_grab_fileid(index_root_t *root, uint16_t fileid) {
    int fd = root->indexfd;

    if(root->indexid != fileid) {
        // the requested datafile is not the current datafile opened
        // we will re-open the expected datafile temporary
        debug("[-] index: switching file: %d, requested: %d\n", root->indexid, fileid);
        if((fd = index_open_file(root, fileid)) < 0)
            return -1;
    }

    return fd;
}

inline void index_release_fileid(index_root_t *root, uint16_t fileid, int fd) {
    // if the requested file id (or fd) is not the one
    // currently used by the main structure, we close it
    // since it was temporary
    if(root->indexid != fileid) {
        close(fd);
    }
}


// convert a binary direct-key to a readable
// direct-key structure
index_dkey_t *index_dkey_from_key(index_dkey_t *dkey, unsigned char *buffer, uint8_t length) {
    if(length != sizeof(index_dkey_t))
        return NULL;

    // binary copy buffer
    memcpy(dkey, buffer, length);

    return dkey;
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

    printf("[+] index: active file: %s\n", root->indexfile);
}

// jumping to the next index id file, this needs to be sync'd with
// data file, we only do this when datafile changes basicly, this is
// triggered by a datafile too big event
size_t index_jump_next(index_root_t *root) {
    hook_t *hook = NULL;

    verbose("[+] index: jumping to the next file\n");

    if(rootsettings.hook) {
        hook = hook_new("jump", 4);
        hook_append(hook, rootsettings.zdbid);
        hook_append(hook, root->indexfile);
    }

    // closing current file descriptor
    close(root->indexfd);

    // moving to the next file
    root->indexid += 1;
    root->nextid = 0;
    root->previous = 0;

    index_set_id(root);

    index_open_final(root);
    index_initialize(root->indexfd, root->indexid, root);

    if(rootsettings.hook) {
        hook_append(hook, root->indexfile);
        hook_execute(hook);
        hook_free(hook);
    }

    if(root->seqid)
        index_seqid_push(root, index_next_id(root), root->indexid);

    return root->indexid;
}

//
// index manipulation
//
uint64_t index_next_id(index_root_t *root) {
    // this is used on sequential-id
    // it gives the next id
    return root->nextentry;
}

uint32_t index_next_objectid(index_root_t *root) {
    // return next object id
    // this id is used on direct mode
    // and is a relative id to local file
    return root->nextid;
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
    index_branch_t *branch = index_branch_get(root->branches, branchkey);
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

// read an index entry from disk
// we assume we know enough data to do everything in a single read call
// which means we need to know offset and id length in advance
//
// this is really important in direct mode to use indirection with index
// to have benefit of compaction etc. and for history support
index_item_t *index_item_get_disk(index_root_t *root, uint16_t indexid, size_t offset, uint8_t idlength) {
    int fd;
    size_t length;
    index_item_t *item;

    // allocate expected entry
    length = sizeof(index_item_t) + idlength;

    if(!(item = malloc(length)))
        return NULL;

    // open requested file
    if((fd = index_open_file(root, indexid)) < 0)
        return NULL;

    // seek to requested offset
    lseek(fd, offset, SEEK_SET);

    // read expected entry
    if(!index_read(fd, item, length)) {
        close(fd);
        free(item);
        return NULL;
    }

    close(fd);

    return item;
}

// insert a key, only in memory, no disk is touched
// this function should be called externaly only when populating something
// if we need to add something new on the index, we should write it on disk
//
// the id on the struct is not really allocated, except if explicity done
// by default, no memory space is allocated (on the stack) for the id, so we need the
// id in external variable, this variable will be copy on the right place if needed
//
// note that we use everything else (including 'idlength') from the new index_entry_t provided
//
index_entry_t *index_entry_update_memory(index_root_t *root, index_entry_t *new, index_entry_t *exists) {
    debug("[+] index: key already exists\n");

    debug("[+] index: flagging previous key as deleted, on disk\n");
    index_entry_delete_disk(root, exists);

    debug("[+] index: updating current entry in memory\n");
    // update statistics
    root->datasize -= exists->length;
    root->datasize += new->length;

    // updating parent id and parent offset
    // to the previous item itself, which
    // will be used to keep track of the history
    exists->parentid = exists->dataid;
    exists->parentoff = exists->idxoffset;

    // re-use existing entry
    exists->length = new->length;
    exists->offset = new->offset;
    exists->flags = new->flags;
    exists->dataid = root->indexid;
    exists->idxoffset = new->idxoffset;
    exists->crc = new->crc;

    return exists;
}

index_entry_t *index_entry_insert_memory(index_root_t *root, unsigned char *realid, index_entry_t *new) {
    // calloc will ensure any unset fields (eg: flags) are zero
    size_t entrysize = sizeof(index_entry_t) + new->idlength;
    index_entry_t *entry = calloc(entrysize, 1);

    memcpy(entry->id, realid, new->idlength);
    entry->idlength = new->idlength;
    entry->namespace = root->namespace;
    entry->offset = new->offset;
    entry->length = new->length;
    entry->dataid = root->indexid;
    entry->idxoffset = new->idxoffset;
    entry->flags = new->flags;
    entry->crc = new->crc;

    uint32_t branchkey = index_key_hash(entry->id, entry->idlength);

    // commit entry into memory
    index_branch_append(root->branches, branchkey, entry);

    // update statistics (if the key exists)
    // maybe it doesn't exists if it comes from a replay
    root->entries += 1;
    root->datasize += new->length;
    root->indexsize += entrysize;

    // update next entry id
    root->nextentry += 1;
    root->nextid += 1;

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
index_entry_t *index_entry_insert_new(index_root_t *root, void *vid, index_entry_t *new, time_t timestamp, index_entry_t *existing) {
    unsigned char *id = (unsigned char *) vid;
    index_entry_t *entry = NULL;
    off_t curoffset = lseek(root->indexfd, 0, SEEK_END);

    // ensure flags are empty
    new->flags = 0;

    // setting the current index offset
    new->idxoffset = curoffset;

    if(existing) {
        if(!(entry = index_entry_update_memory(root, new, existing)))
            return NULL;

    } else {
        if(!(entry = index_entry_insert_memory(root, id, new)))
            return NULL;
    }

    size_t entrylength = sizeof(index_item_t) + entry->idlength;

    memcpy(index_transition->id, entry->id, entry->idlength);
    index_transition->idlength = entry->idlength;
    index_transition->offset = entry->offset;
    index_transition->length = entry->length;
    index_transition->flags = entry->flags;
    index_transition->dataid = entry->dataid;
    index_transition->timestamp = timestamp;
    index_transition->previous = root->previous;
    index_transition->parentid = entry->parentid;
    index_transition->parentoff = entry->parentoff;
    index_transition->crc = entry->crc;

    // updating global previous
    root->previous = curoffset;

    if(!index_write(root->indexfd, index_transition, entrylength, root)) {
        fprintf(stderr, "[-] index: cannot write index entry on disk\n");

        // it's easier to flag the entry as deleted than
        // removing it from the list
        entry->flags |= INDEX_ENTRY_DELETED;

        return NULL;
    }

    return entry;
}

// IMPORTANT:
//   this function is the only one to 'break' the always append
//   behavior, this function will overwrite existing index by
//   seeking and rewrite header, **only** in direct-mode
//
// when deleting some data, we mark (flag) this data as deleted which
// allows two things
//   - we can do compaction offline by removing theses blocks
//   - since direct-key mode use keys which contains file location's
//     dependant information, we can't do anything else than updating
//     existing data
//     we do this in the index file and not in the data file so we keep
//     the data file really always append in any case
int index_entry_delete_memory(index_root_t *root, index_entry_t *entry) {
    // running in a mode without index, let's just skip this
    if(root->branches == NULL)
        return 0;

    uint32_t branchkey = index_key_hash(entry->id, entry->idlength);
    index_branch_t *branch = index_branch_get(root->branches, branchkey);
    index_entry_t *previous = index_branch_get_previous(branch, entry);

    debug("[+] index: delete memory: removing entry from memory\n");

    if(previous == entry) {
        danger("[-] index: entry delete memory: something wrong happens");
        danger("[-] index: entry delete memory: branches seems buggy");
        return 1;
    }

    // removing entry from global branch
    index_branch_remove(branch, entry, previous);

    // updating statistics
    root->entries -= 1;
    root->datasize -= entry->length;
    root->indexsize -= sizeof(index_entry_t) + entry->idlength;

    // cleaning memory object
    free(entry);

    return 0;
}

int index_entry_delete_disk(index_root_t *root, index_entry_t *entry) {
    int fd;

    // compute this object size on disk
    size_t entrylength = sizeof(index_item_t) + entry->idlength;

    // mark entry as deleted
    // this affect the memory object (runtime)
    entry->flags |= INDEX_ENTRY_DELETED;

    // (re-)open the expected index file, in read-write mode
    if((fd = index_open_file_rw(root, entry->dataid)) < 0)
        return 1;

    // jump to the right offset for this entry
    debug("[+] index: delete: reading %lu bytes at offset %" PRIu32 "\n", entrylength, entry->idxoffset);
    lseek(fd, entry->idxoffset, SEEK_SET);

    // reading the exact entry from disk
    if(read(fd, index_transition, entrylength) != (ssize_t) entrylength) {
        warnp("index_entry_delete read");
        close(fd);
        return 1;
    }

    index_item_header_dump(index_transition);

    // update the flags
    index_transition->flags = entry->flags;

    // rollback to point to the entry again
    debug("[+] index: delete: overwriting key\n");
    lseek(fd, entry->idxoffset, SEEK_SET);

    // overwrite the key
    if(write(fd, index_transition, entrylength) != (ssize_t) entrylength) {
        warnp("index_entry_delete write");
        close(fd);
        return 1;
    }

    return 0;
}

int index_entry_delete(index_root_t *root, index_entry_t *entry) {
    // first flag disk entry as deleted
    if(index_entry_delete_disk(root, entry))
        return 1;

    // then remove entry from memory
    if(index_entry_delete_memory(root, entry))
        return 1;

    return 0;
}

// serialize into binary object a deserializable
// object identifier
index_bkey_t index_item_serialize(index_item_t *item, uint32_t idxoffset) {
    index_bkey_t key = {
        .idlength = item->idlength,
        .fileid = item->dataid,
        .length = item->length,
        .idxoffset = idxoffset,
        .crc = item->crc
    };

    return key;
}

// read an object from disk, based on binarykey provided
// and ensure the key object seems legit with the requested key
index_entry_t *index_entry_deserialize(index_root_t *root, index_bkey_t *key) {
    index_item_t *item;

    if(!(item = index_item_get_disk(root, key->fileid, key->idxoffset, key->idlength)))
        return NULL;

    if(item->length != key->length || item->crc != key->crc) {
        debug("[-] index: deserialize: invalid key requested (fields mismatch)\n");
        free(item);
        return NULL;
    }

    // FIXME: avoid double malloc for a single object
    index_entry_t *entry;

    if(!(entry = malloc(sizeof(index_entry_t) + item->idlength))) {
        debug("[-] index: deserialize: cannot allocate memory\n");
        free(item);
        return NULL;
    }

    entry->idlength = item->idlength;
    entry->offset = item->offset;
    entry->dataid = item->dataid;
    entry->flags = item->flags;
    entry->idxoffset = key->idxoffset;
    entry->crc = item->crc;
    entry->length = item->length;
    memcpy(entry->id, item->id, item->idlength);

    // clean intermediate item
    free(item);

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

// returns offset in the indexfile from idobject
size_t index_offset_objectid(uint32_t objectid) {
    // skip index header
    size_t offset = sizeof(index_header_t);

    // index is linear like this
    // [header][obj-1][obj-2][obj-3][...]
    //
    // object X offset can be found by computing
    // size of each entry, in direct mode, keys are
    // always fixed-length
    //
    //                       each entry          fixed-key-length
    offset += (objectid * (sizeof(index_item_t) + sizeof(index_dkey_t)));

    return offset;
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

