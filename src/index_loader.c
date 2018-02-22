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
#include <errno.h>
#include "zerodb.h"
#include "index.h"
#include "index_loader.h"
#include "index_branch.h"
#include "data.h"

//
// index initializer and dumper
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
    printf("[+] key [");
    hexdump(entry->id, entry->idlength);
    printf("] offset %" PRIu64 ", length: %" PRIu64 "\n", entry->offset, entry->length);
}

// dumps the current index load
// fulldump flags enable printing each entry
static void index_dump(index_root_t *root, int fulldump) {
    size_t datasize = 0;
    size_t entries = 0;
    size_t indexsize = 0;
    size_t branches = 0;

    printf("[+] verifyfing index populated\n");

    if(fulldump)
        printf("[+] ===========================\n");

    // iterating over each buckets
    for(uint32_t b = 0; b < buckets_branches; b++) {
        index_branch_t *branch = index_branch_get(root, b);

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
    size_t overhead = (buckets_branches * sizeof(index_branch_t **)) +
                      (branches * sizeof(index_branch_t));

    verbose("[+] memory overhead: %.2f KB (%lu bytes)\n", (overhead / 1024.0), overhead);
}

//
// initialize an index file
// this basicly create the header and write it
//
index_t index_initialize(int fd, uint16_t indexid) {
    index_t header;

    memcpy(header.magic, "IDX0", 4);
    header.version = 1;
    header.created = time(NULL);
    header.fileid = indexid;
    header.opened = time(NULL);
    header.mode = rootsettings.mode;

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
        root->status |= INDEX_READ_ONLY;
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
            warnp("index: header read");
            root->status |= INDEX_DEGRADED;
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
        if(root->status & INDEX_READ_ONLY) {
            fprintf(stderr, "[-] no index found and readonly filesystem\n");
            fprintf(stderr, "[-] cannot starts correctly\n");
            exit(EXIT_FAILURE);
        }

        printf("[+] creating empty index file\n");
        header = index_initialize(root->indexfd, root->indexid);
    }


    // re-writing the header, with updated data if the index is writable
    // if the file was just created, it's okay, we have a new struct ready
    if(!(root->status & INDEX_READ_ONLY)) {
        // updating index with latest opening state
        header.opened = time(NULL);
        lseek(root->indexfd, 0, SEEK_SET);

        if(!index_write(root->indexfd, &header, sizeof(index_t)))
            diep(root->indexfile);
    }

    char date[256];
    verbose("[+] index created at: %s\n", index_date(header.created, date, sizeof(date)));
    verbose("[+] index last open: %s\n", index_date(header.opened, date, sizeof(date)));

    if(header.mode != rootsettings.mode) {
        danger("[!] ========================================================");
        danger("[!] DANGER: index created in another mode than running mode");
        danger("[!] DANGER: unexpected result could occures, be careful");
        danger("[!] ========================================================");
    }

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
        root->nextentry += 1;
    }

    free(entry);

    close(root->indexfd);

    // if length is greater than 0, the index was existing
    // if length is 0, index just has been created
    return length;
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

    if(root->status & INDEX_READ_ONLY) {
        warning("[-] ========================================================");
        warning("[-] WARNING: running in read-only mode");
        warning("[-] WARNING: index filesystem is not writable");
        warning("[-] ========================================================");
    }

    if(root->status & INDEX_DEGRADED) {
        warning("[-] ========================================================");
        warning("[-] WARNING: index degraded (read errors)");
        warning("[-] ========================================================");
    }

    if(root->status & INDEX_HEALTHY)
        success("[+] index healthy");

    // setting index as loaded (removing flag)
    root->status &= ~INDEX_NOT_LOADED;

    // opening the real active index file in append mode
    index_open_final(root);
}

// create an index and load files
uint16_t index_init(settings_t *settings) {
    index_root_t *root = calloc(sizeof(index_root_t), 1);

    debug("[+] initializing index\n");

    root->indexdir = settings->indexpath;
    root->indexid = 0;
    root->indexfile = malloc(sizeof(char) * (PATH_MAX + 1));
    root->nextentry = 0;
    root->sync = settings->sync;
    root->synctime = settings->synctime;
    root->lastsync = 0;
    root->status = INDEX_NOT_LOADED | INDEX_HEALTHY;

    // don't allocate branch on direct-key mode since the
    // index is not used (no lookup needed)
    // we don't load index neither, since the index will always
    // be empty
    if(settings->mode == KEYVALUE || settings->mode == SEQUENTIAL) {
        debug("[+] allocating index (%d lazy branches)\n", buckets_branches);

        // allocating minimal branches array
        if(!(root->branches = (index_branch_t **) calloc(sizeof(index_branch_t *), buckets_branches)))
            diep("calloc");

        // allocating transition variable, a reusable item
        if(!(index_transition = malloc(sizeof(index_item_t) + MAX_KEY_LENGTH + 1)))
            diep("malloc");

    } else if(settings->mode == DIRECTKEY) {
        // in direct key mode, we allocate a re-usable
        // index_entry_t which will be adapted each time
        // using the requested key, like this we can use the same
        // implementation for everything
        //
        // in the default mode, the key is always kept in memory
        // and never free'd, that's why we will allocate a one-time
        // object now and reuse the same all the time
        if(!(index_reusable_entry = (index_entry_t *) malloc(sizeof(index_entry_t))))
            diep("malloc");
    }

    // commit variable
    // FIXME: this needs to get away, no more global
    //        variable for index/data management
    index_set_rootindex(root);

    index_load(root);

    if(settings->mode == KEYVALUE || settings->mode == SEQUENTIAL)
        index_dump(root, settings->dump);

    return root->indexid;
}

