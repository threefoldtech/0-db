#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <time.h>
#include <errno.h>
#include "libzdb.h"
#include "libzdb_private.h"

//
// index initializer and dumper
//
static inline void index_dump_entry(index_entry_t *entry) {
    printf("[+] key [");
    zdb_hexdump(entry->id, entry->idlength);
    printf("] offset %" PRIu32 ", length: %" PRIu32 "\n", entry->offset, entry->length);
}

// dumps the current index load
// fulldump flags enable printing each entry
static void index_dump(index_root_t *root, int fulldump) {
    size_t branches = 0;

    printf("[+] index: verifyfing populated keys\n");

    if(fulldump)
        printf("[+] ===========================\n");

    // iterating over each buckets
    for(uint32_t b = 0; b < buckets_branches; b++) {
        index_branch_t *branch = index_branch_get(root->branches, b);

        // skipping empty branch
        if(!branch)
            continue;

        branches += 1;
        index_entry_t *entry = branch->list;

        if(!fulldump)
            continue;

        // iterating over the linked-list
        for(; entry; entry = entry->next)
            index_dump_entry(entry);
    }

    if(fulldump) {
        if(root->entries == 0)
            printf("[+] index is empty\n");

        printf("[+] ===========================\n");
    }

    zdb_verbose("[+] index: uses: %lu branches\n", branches);

    // overhead contains:
    // - the buffer allocated to hold each (futur) branches pointer
    // - the branch struct itself for each branches
    size_t overhead = (buckets_branches * sizeof(index_branch_t **)) +
                      (branches * sizeof(index_branch_t));

    zdb_verbose("[+] index: memory overhead: %.2f KB (%lu bytes)\n", KB(overhead), overhead);
}

static void index_dump_statistics(index_root_t *root) {
    zdb_verbose("[+] index: load: %lu entries\n", root->entries);

    double datamb = MB(root->datasize);
    double indexkb = KB(root->indexsize);

    zdb_verbose("[+] index: datasize: " COLOR_CYAN "%.2f MB" COLOR_RESET " (%lu bytes)\n", datamb, root->datasize);
    zdb_verbose("[+] index: raw usage: %.1f KB (%lu bytes)\n", indexkb, root->indexsize);
}

//
// initialize an index file
// this basicly create the header and write it
//
index_header_t index_initialize(int fd, uint16_t indexid, index_root_t *root) {
    index_header_t header;

    memcpy(header.magic, "IDX0", 4);
    header.version = ZDB_IDXFILE_VERSION;
    header.created = time(NULL);
    header.fileid = indexid;
    header.opened = time(NULL);
    header.mode = zdb_rootsettings.mode;

    if(!index_write(fd, &header, sizeof(index_header_t), root))
        zdb_diep("index_initialize: write");

    return header;
}

static int index_try_rootindex(index_root_t *root) {
    // try to open the index file with create flag
    if((root->indexfd = open(root->indexfile, O_CREAT | O_RDWR, 0600)) < 0) {
        // okay it looks like we can't open this file
        // the only case we support is if the filesystem is in
        // read only, otherwise we just crash, this should not happen
        if(errno != EROFS)
            zdb_diep(root->indexfile);

        zdb_debug("[-] warning: read-only index filesystem\n");

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
            zdb_diep(root->indexfile);
        }

        // we keep track that we are on a readonly filesystem
        // we can't live with it, but with restriction
        root->status |= INDEX_READ_ONLY;
    }

    return 1;
}

index_header_t *index_descriptor_load(index_root_t *root) {
    index_header_t *header;
    ssize_t length;

    if(!(header = malloc(sizeof(index_header_t)))) {
        zdb_warnp("index_descriptor_load: malloc");
        return NULL;
    }

    lseek(root->indexfd, 0, SEEK_SET);

    if((length = read(root->indexfd, header, sizeof(index_header_t))) != sizeof(index_header_t)) {
        free(header);
        return NULL;
    }

    return header;
}

index_header_t *index_descriptor_validate(index_header_t *header, index_root_t *root) {
    if(memcmp(header->magic, "IDX0", 4)) {
        zdb_danger("[-] %s: invalid header, wrong magic", root->indexfile);
        return NULL;
    }

    if(header->version != ZDB_IDXFILE_VERSION) {
        zdb_danger("[-] %s: unsupported version detected", root->indexfile);
        zdb_danger("[-] file version: %d, supported version: %d", header->version, ZDB_IDXFILE_VERSION);
        return NULL;
    }

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
    index_header_t header;
    ssize_t length;

    zdb_verbose("[+] index: loading file: %s\n", root->indexfile);

    if(!index_try_rootindex(root))
        return 0;

    if((length = read(root->indexfd, &header, sizeof(index_header_t))) != sizeof(index_header_t)) {
        //
        // FIXME: replace this with helper
        //

        if(length < 0) {
            // read failed, probably caused by a system error
            // this is probably an unrecoverable issue, let's skip this
            // index file (this could break consistancy)
            zdb_warnp("index: header read");
            root->status |= INDEX_DEGRADED;
            return 1;
        }

        if(length > 0) {
            // we read something, but not the expected header, at least
            // not this amount of data, which is a completly undefined behavior
            // let's just stopping here
            fprintf(stderr, "[-] index: header corrupted or incomplete\n");
            fprintf(stderr, "[-] index: expected %lu bytes, %ld read\n", sizeof(index_header_t), length);
            exit(EXIT_FAILURE);
        }

        // we could not read anything, which basicly means that
        // the file is empty, we probably just created it
        //
        // if the current indexid is not zero, this is probably
        // a new file not expected, otherwise if index is zero,
        // this is the initial index file we need to create
        if(root->indexid > 0) {
            zdb_verbose("[+] index: discarding file\n");
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

        printf("[+] index: creating empty file\n");
        header = index_initialize(root->indexfd, root->indexid, root);
    }

    if(!index_descriptor_validate(&header, root))
        return 1;

    // re-writing the header, with updated data if the index is writable
    // if the file was just created, it's okay, we have a new struct ready
    if(!(root->status & INDEX_READ_ONLY)) {
        // updating index with latest opening state
        header.opened = time(NULL);
        lseek(root->indexfd, 0, SEEK_SET);

        if(!index_write(root->indexfd, &header, sizeof(index_header_t), root))
            zdb_diep(root->indexfile);
    }

    char date[256];
    zdb_verbose("[+] index: created at: %s\n", zdb_header_date(header.created, date, sizeof(date)));
    zdb_verbose("[+] index: last open: %s\n", zdb_header_date(header.opened, date, sizeof(date)));

    if(header.mode != zdb_rootsettings.mode) {
        zdb_danger("[!] ========================================================");
        zdb_danger("[!] DANGER: index created in another mode than running mode");
        zdb_danger("[!] DANGER: stopping here, to ensure no data loss");
        zdb_danger("[!] ========================================================");

        exit(EXIT_FAILURE);
    }

    printf("[+] index: populating: %s\n", root->indexfile);

    // index seems in a good state
    // let's load it completely in memory now
    char *filebuf;
    off_t fullsize = lseek(root->indexfd, 0, SEEK_END);

    zdb_debug("[+] index: loading in memory file: %.2f MB\n", MB(fullsize));

    if(!(filebuf = malloc(fullsize)))
        zdb_diep("index buffer: malloc");

    lseek(root->indexfd, 0, SEEK_SET);
    if(read(root->indexfd, filebuf, fullsize) != fullsize)
        zdb_diep("index buffer: read");

    // positioning seeker to beginin of index entries
    char *initseeker = filebuf + sizeof(index_header_t);
    char *seeker = initseeker;

    // reading the index, populating memory
    //
    // here it's again a little bit dirty
    // we assume that key length is maximum 256 bytes, we stored this
    // size in a uint8_t, that means that for knowing each entry size, we
    // need to know the id length, which is the first field of the struct
    index_item_t *entry = NULL;

    // ensure nextid is zero, because this id
    // is relative to the indexfile, we start to populate
    // this file, starting from zero
    root->nextid = 0;

    while(seeker < filebuf + fullsize) {
        index_entry_t *fresh = NULL;

        entry = (index_item_t *) seeker;
        off_t offset = seeker - filebuf;

        // create a gateway struct to fill our index memory
        // this is not nice (lot of copy) but make things more
        // generic and clear
        index_entry_t source = {
            .idlength = entry->idlength,
            .indexid = root->indexid,
            // WARNING: missing dataid ?
            .length = entry->length,
            .offset = entry->offset,
            .flags = entry->flags,
            .idxoffset = offset,
            .crc = entry->crc,
            .parentid = entry->parentid,
            .parentoff = entry->parentoff,
        };

        // checking if we are in sequential mode
        // and this if the first key, we need to populate
        // our mapping with this key
        //
        // the set operation will update 'nextentry' counter
        // we need to update seqid before inserting key
        if(root->seqid && seeker == initseeker) {
            index_seqid_push(root, root->nextentry, root->indexid);
            // index_seqid_dump(root);
        }

        // insert this entry like it was inserted by a user
        // this allows us to keep a generic way of inserting data and keeping a
        // single point of logic when adding data (logic for overwrite, resize bucket, ...)
        fresh = index_set_memory(root, entry->id, &source);

        // now we added the entry (whatever it was)
        // if this entry was flagged as deleted, let simulate a deletion
        // like it was (we do replay here), this ensure coherence of data
        //
        // we can't just skip deleted entries, otherwise previously
        // inserted data won't be flagged as deleted
        if(index_entry_is_deleted(fresh))
            index_entry_delete_memory(root, fresh);

        // set the previous pointing to this entry
        // this is the last one we added
        root->previous = seeker - filebuf;

        // moving seeker to next entry in the buffer
        seeker += sizeof(index_item_t) + entry->idlength;
    }

    zdb_debug("[+] index: last offset: %lu\n", root->previous);

    // freeing buffer memory
    free(filebuf);

    // this file is done
    close(root->indexfd);

    // if length is greater than 0, the index was existing
    // if length is 0, index just has been created
    return length;
}

// returns the amount of index files available (if any)
uint64_t index_availity_check(index_root_t *root) {
    uint64_t maxfiles = (1 << (sizeof(((index_entry_t *) 0)->dataid) * 8));
    uint64_t fileid;

    for(fileid = 0; fileid < maxfiles; fileid++) {
        index_set_id(root, fileid);

        // if we can't open fileid 0, we don't have any index
        // file available (returns 0)
        //
        // if we can at least open fileid 0, we have at least 1 index
        // file available (fileid 1, ...)
        if(zdb_file_exists(root->indexfile) != ZDB_FILE_EXISTS)
            break;
    }

    return fileid;
}

// load all the index found
// if no index files exists, we create the original one
void index_internal_load(index_root_t *root) {
    uint64_t maxfile = index_availity_check(root);
    uint64_t fileid;

    if(maxfile > 0) {
        // opening all index files one by one
        for(fileid = 0; fileid < maxfile; fileid++) {
            index_set_id(root, fileid);

            if(index_load_file(root) == 0) {
                zdb_verbose("[-] index_load: something went wrong with index %d\n", root->indexid);
                break;
            }
        }

    } else {
        // we need to create the index
        index_set_id(root, 0);
        if(index_load_file(root) != 0) {
            zdb_verbose("[-] index_load: seems initial index could not be created\n");
            return;
        }
    }

    if(root->seqid && root->seqid->length == 0) {
        zdb_debug("[+] index: fresh database created in sequential mode\n");
        zdb_debug("[+] index: initializing default seqmap\n");
        index_seqid_push(root, 0, 0);
    }

    if(root->status & INDEX_READ_ONLY) {
        zdb_warning("[-] ========================================================");
        zdb_warning("[-] WARNING: running in read-only mode");
        zdb_warning("[-] WARNING: index filesystem is not writable");
        zdb_warning("[-] ========================================================");
    }

    if(root->status & INDEX_DEGRADED) {
        zdb_warning("[-] ========================================================");
        zdb_warning("[-] WARNING: index degraded (read errors)");
        zdb_warning("[-] ========================================================");
    }

    if(root->status & INDEX_HEALTHY)
        zdb_success("[+] index: healthy");

    // setting index as loaded (removing flag)
    root->status &= ~INDEX_NOT_LOADED;

    // opening the real active index file in append mode
    index_open_final(root);
}

void index_internal_allocate_single() {
    // if variables are already allocated
    // this process is silently skipped

    // always allocating the transition keys, since all mode use at
    // least the index handlers (all the work to avoid index branch in
    // direct-key mode is done on the branch code)

    // avoid already allocated buffer
    if(index_transition)
        return;

    // allocating transition variable, a reusable item
    if(!(index_transition = malloc(sizeof(index_item_t) + MAX_KEY_LENGTH + 1)))
        zdb_diep("malloc");

    // avoid already allocated buffer
    if(index_reusable_entry)
        return;

    // in direct key mode, more over, we allocate a re-usable
    // index_entry_t which will be adapted each time
    // using the requested key, like this we can use the same
    // implementation for everything
    //
    // in the default mode, the key is always kept in memory
    // and never free'd, that's why we will allocate a one-time
    // object now and reuse the same all the time
    //
    // this is allocated, when index mode can be different on runtime
    if(!(index_reusable_entry = (index_entry_t *) malloc(sizeof(index_entry_t) + MAX_KEY_LENGTH)))
        zdb_diep("malloc");
}

index_seqid_t *index_allocate_seqid() {
    index_seqid_t *seqid;

    zdb_debug("[+] index loader: allocating sequential buffer map\n");
    if(!(seqid = malloc(sizeof(index_seqid_t))))
        zdb_diep("index loader: seqid: malloc");

    seqid->allocated = 1024;
    seqid->length = 0;

    if(!(seqid->seqmap = malloc(seqid->allocated * sizeof(index_seqmap_t))))
        zdb_diep("index loader: seqid: buffer malloc");

    return seqid;
}

index_root_t *index_init_lazy(zdb_settings_t *settings, char *indexdir, void *namespace) {
    index_root_t *root;

    if(!(root = calloc(sizeof(index_root_t), 1))) {
        zdb_diep("index: calloc");
        return NULL;
    }

    root->indexdir = indexdir;
    root->indexid = 0;
    root->indexfile = malloc(sizeof(char) * (ZDB_PATH_MAX + 1));
    root->nextentry = 0;
    root->nextid = 0;
    root->previous = 0;
    root->sync = settings->sync;
    root->synctime = settings->synctime;
    root->lastsync = 0;
    root->status = INDEX_NOT_LOADED | INDEX_HEALTHY;
    root->branches = NULL;
    root->namespace = namespace;
    root->mode = settings->mode;

    return root;
}

// create an index and load files
index_root_t *index_init(zdb_settings_t *settings, char *indexdir, void *namespace, index_branch_t **branches) {
    zdb_debug("[+] index: initializing\n");

    index_root_t *root = index_init_lazy(settings, indexdir, namespace);
    root->branches = branches;

    if(settings->mode == ZDB_MODE_SEQUENTIAL)
        root->seqid = index_allocate_seqid();

    // since this function will be called for each namespace
    // we will not allocate all the time the reusable variables
    // but this is the 'main entry' of index loading, so doing this
    // here makes sens
    index_internal_allocate_single();
    index_internal_load(root);

    if(settings->mode == ZDB_MODE_KEY_VALUE)
        index_dump(root, settings->dump);

    index_dump_statistics(root);

    #ifndef RELEASE
    if(settings->mode == ZDB_MODE_SEQUENTIAL)
        index_seqid_dump(root);
    #endif

    return root;
}

// graceful clean everything allocated
// by this loader
void index_destroy(index_root_t *root) {
    // delete root object
    free(root->indexfile);

    if(root->seqid) {
        free(root->seqid->seqmap);
        free(root->seqid);
    }

    free(root);
}

// clean single-time allocated variable
// this will be called a single time when we're sure
// nothing will be used anymore on any indexes
// (basicly graceful shutdown)
void index_destroy_global() {
    free(index_transition);
    index_transition = NULL;

    free(index_reusable_entry);
    index_reusable_entry = NULL;
}

// delete index files (not the namespace descriptor)
void index_delete_files(index_root_t *root) {
    zdb_dir_clean_payload(root->indexdir);
}
