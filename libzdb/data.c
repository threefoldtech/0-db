#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <x86intrin.h>
#include <errno.h>
#include <time.h>
#include "libzdb.h"
#include "libzdb_private.h"

#if 0
// dump a data entry
static void data_entry_header_dump(data_entry_header_t *entry) {
#ifdef RELEASE
    (void) entry;
#else
    zdb_debug("[+] data: entry dump: id length  : %u\n", entry->idlength);
    zdb_debug("[+] data: entry dump: data length: %u\n", entry->datalength);
    zdb_debug("[+] data: entry dump: previous   : %u\n", entry->previous);
    zdb_debug("[+] data: entry dump: integrity  : %X\n", entry->integrity);
    zdb_debug("[+] data: entry dump: flags      : %u\n", entry->flags);
    zdb_debug("[+] data: entry dump: timestamp  : %u\n", entry->timestamp);
#endif
}
#endif

// force to sync data buffer into the underlaying device
static inline int data_sync(data_root_t *root, int fd) {
    fsync(fd);
    root->lastsync = time(NULL);
    return 1;
}

// checking is some sync is forced
// there is two possibilities:
// - we set --sync option on runtime, and each write is sync forced
// - we set --synctime on runtime and after this amount of seconds
//   we force to sync the last write
static inline int data_sync_check(data_root_t *root, int fd) {
    if(root->sync)
        return data_sync(root, fd);

    if(!root->synctime)
        return 0;

    if((time(NULL) - root->lastsync) > root->synctime) {
        zdb_debug("[+] data: last sync expired, force sync\n");
        return data_sync(root, fd);
    }

    return 0;
}

// wrap (mostly) all write operation on datafile
// it's easier to keep a single logic with error handling
// related to write check
// this function takes an extra argument 'syncer" which explicitly
// ask to check if we need to do some sync-check or not
// this is useful when writing data, we write header then payload
// and we can avoid to do two sync (only one when done)
static int data_write(int fd, void *buffer, size_t length, int syncer, data_root_t *root) {
    ssize_t response;

    if((response = write(fd, buffer, length)) < 0) {
        // update statistics
        zdb_rootsettings.stats.datawritefailed += 1;

        zdb_warnp("data write");
        return 0;
    }

    if(response != (ssize_t) length) {
        fprintf(stderr, "[-] data write: partial write\n");
        return 0;
    }

    // update statistics
    zdb_rootsettings.stats.datadiskwrite += length;

    if(syncer)
        data_sync_check(root, fd);

    return 1;
}

// open one datafile based on it's id
// in case of error, the reason will be printed and -1 will be returned
// otherwise the file descriptor is returned
int data_open_id_mode(data_root_t *root, uint16_t id, int mode) {
    char temp[ZDB_PATH_MAX];
    int fd;

    sprintf(temp, "%s/zdb-data-%05u", root->datadir, id);
    zdb_debug("[+] data: opening file: %s (ro: %s)\n", temp, (mode & O_RDONLY) ? "yes" : "no");

    if((fd = open(temp, mode, 0600)) < 0) {
        zdb_warnp(temp);
        return -1;
    }

    return fd;
}

// default mode, read-only datafile
static int data_open_id(data_root_t *root, uint16_t id) {
    return data_open_id_mode(root, id, O_RDONLY);
}

// since data are **really** always append
// there is no more reason to keep a read-write function
// let keep it here for history reason
#if 0
// special case (for deletion) where read-write is needed
// and not in append mode
static int data_get_dataid_rw(data_root_t *root, uint16_t id) {
    return data_open_id_mode(root, id, O_RDWR);
}
#endif

data_header_t *data_descriptor_load(data_root_t *root) {
    data_header_t *header;
    ssize_t length;

    if(!(header = malloc(sizeof(data_header_t)))) {
        zdb_warnp("data_descriptor_load: malloc");
        return NULL;
    }

    lseek(root->datafd, 0, SEEK_SET);

    if((length = read(root->datafd, header, sizeof(data_header_t))) != sizeof(data_header_t)) {
        free(header);
        return NULL;
    }

    return header;
}

data_header_t *data_descriptor_validate(data_header_t *header, data_root_t *root) {
    if(memcmp(header->magic, "DAT0", 4)) {
        zdb_danger("[-] %s: invalid header, wrong magic", root->datafile);
        return NULL;
    }

    if(header->version != ZDB_DATAFILE_VERSION) {
        zdb_danger("[-] %s: unsupported version detected", root->datafile);
        zdb_danger("[-] file version: %d, supported version: %d", header->version, ZDB_DATAFILE_VERSION);
        return NULL;
    }

    return header;
}

// main function to call when you need to deal with data id
// this function takes care to open the right file id:
//  - if you want the current opened file id, you have thid fd
//  - if the file is not opened yet, you'll receive a new fd
// you need to call the .... to be consistant about cleaning this
// file open, if a new one was opened
//
// if the data id could not be opened, -1 is returned
static inline int data_grab_dataid(data_root_t *root, uint16_t dataid) {
    int fd = root->datafd;

    if(root->dataid != dataid) {
        // the requested datafile is not the current datafile opened
        // we will re-open the expected datafile temporary
        zdb_debug("[-] data: switching file: %d, requested: %d\n", root->dataid, dataid);
        if((fd = data_open_id(root, dataid)) < 0)
            return -1;
    }

    return fd;
}

static inline void data_release_dataid(data_root_t *root, uint16_t dataid, int fd) {
    // if the requested data id (or fd) is not the one
    // currently used by the main structure, we close it
    // since it was temporary
    if(root->dataid != dataid) {
        close(fd);
    }
}

//
// data management
//
void data_initialize(char *filename, data_root_t *root) {
    int fd;

    if((fd = open(filename, O_CREAT | O_RDWR, 0600)) < 0) {
        // ignoring initializer on read-only filesystem
        if(errno == EROFS)
            return;

        zdb_diep(filename);
    }

    // writing initial header
    data_header_t header;

    memcpy(header.magic, "DAT0", 4);
    header.version = ZDB_DATAFILE_VERSION;
    header.created = time(NULL);
    header.opened = 0; // not supported yet
    header.fileid = root->dataid;

    if(!data_write(fd, &header, sizeof(data_header_t), 1, root))
        zdb_diep(filename);

    close(fd);
}

// simply set globaly the current filename based on it's id
static void data_set_id(data_root_t *root) {
    sprintf(root->datafile, "%s/zdb-data-%05u", root->datadir, root->dataid);
}

static void data_open_final(data_root_t *root) {
    // try to open the datafile in write mode to append new data
    if((root->datafd = open(root->datafile, O_CREAT | O_RDWR | O_APPEND, 0600)) < 0) {
        // maybe we are on a read-only filesystem
        // let's try to open it in read-only
        if(errno != EROFS)
            zdb_diep(root->datafile);

        if((root->datafd = open(root->datafile, O_RDONLY, 0600)) < 0)
            zdb_diep(root->datafile);

        zdb_debug("[+] data: file opened in read-only mode\n");
    }

    // jumping to the first entry
    lseek(root->datafd, sizeof(data_header_t), SEEK_SET);

    // reading all indexes to find where is the last one
    data_entry_header_t header;
    int entries = 0;

    zdb_debug("[+] data: reading file, finding last entry\n");

    while(read(root->datafd, &header, sizeof(data_entry_header_t)) == sizeof(data_entry_header_t)) {
        root->previous = lseek(root->datafd, 0, SEEK_CUR) - sizeof(data_entry_header_t);
        lseek(root->datafd, header.datalength + header.idlength, SEEK_CUR);

        entries += 1;
    }

    zdb_debug("[+] data: entries read: %d, last offset: %lu\n", entries, root->previous);
    printf("[+] data: active file: %s\n", root->datafile);
}

// jumping to the next id close the current data file
// and open the next id file, it will create the new file
size_t data_jump_next(data_root_t *root, uint16_t newid) {
    zdb_verbose("[+] data: jumping to the next file\n");

    // closing current file descriptor
    close(root->datafd);

    // moving to the next file
    root->dataid = newid;
    data_set_id(root);

    data_initialize(root->datafile, root);
    data_open_final(root);

    return root->dataid;
}

// compute a crc32 of the payload
// this function uses Intel CRC32 (SSE4.2) intrinsic
uint32_t data_crc32(const uint8_t *bytes, ssize_t length) {
    uint64_t *input = (uint64_t *) bytes;
    uint32_t hash = 0;
    ssize_t i = 0;

    for(i = 0; i < length - 8; i += 8)
        hash = _mm_crc32_u64(hash, *input++);

    for(; i < length; i++)
        hash = _mm_crc32_u8(hash, bytes[i]);

    return hash;
}

static size_t data_length_from_offset(int fd, size_t offset) {
    data_entry_header_t header;

    // moving the the header offset
    lseek(fd, offset, SEEK_SET);

    if(read(fd, &header, sizeof(data_entry_header_t)) != sizeof(data_entry_header_t)) {
        zdb_warnp("data header read");
        return 0;
    }

    return header.datalength;
}

// real data_get implementation
static inline data_payload_t data_get_real(int fd, size_t offset, size_t length, uint8_t idlength) {
    data_payload_t payload = {
        .buffer = NULL,
        .length = 0
    };

    // if we don't know the length in advance, we will read the
    // data header to know the payload size from it
    if(length == 0) {
        zdb_debug("[+] data: fetching length from datafile\n");

        if((length = data_length_from_offset(fd, offset)) == 0) {
            payload.buffer = malloc(0);
            return payload;
        }

        zdb_debug("[+] data: length from datafile: %zu\n", length);
    }

    // positioning datafile to expected offset
    // and skiping header (pointing to payload)
    lseek(fd, offset + sizeof(data_entry_header_t) + idlength, SEEK_SET);

    // allocating buffer from length
    // (from index or data header, we don't care)
    payload.buffer = malloc(length);
    payload.length = length;

    if(read(fd, payload.buffer, length) != (ssize_t) length) {
        zdb_rootsettings.stats.datareadfailed += 1;
        zdb_warnp("data_get: read");

        free(payload.buffer);
        payload.buffer = NULL;
    }

    // update statistics
    zdb_rootsettings.stats.datadiskread += length;

    return payload;
}

// wrapper for data_get_real, which open the right dataid
// which allows to do only the needed and this wrapper prepare the right data id
data_payload_t data_get(data_root_t *root, size_t offset, size_t length, uint16_t dataid, uint8_t idlength) {
    int fd;
    data_payload_t payload = {
        .buffer = NULL,
        .length = 0
    };

    // acquire data id fd
    if((fd = data_grab_dataid(root, dataid)) < 0)
        return payload;

    payload = data_get_real(fd, offset, length, idlength);

    // release dataid
    data_release_dataid(root, dataid, fd);

    return payload;
}


// check payload integrity from any datafile
// real implementation
static inline int data_check_real(int fd, size_t offset) {
    unsigned char *buffer;
    data_entry_header_t header;

    // positioning datafile to expected offset
    lseek(fd, offset, SEEK_SET);

    if(read(fd, &header, sizeof(data_entry_header_t)) != (ssize_t) sizeof(data_entry_header_t)) {
        zdb_warnp("data: checker: header read");
        return -1;
    }

    // skipping the key, set buffer to payload point
    lseek(fd, header.idlength, SEEK_CUR);

    // allocating buffer from header's length
    buffer = malloc(header.datalength);

    if(read(fd, buffer, header.datalength) != (ssize_t) header.datalength) {
        // update statistics
        zdb_rootsettings.stats.datareadfailed += 1;

        zdb_warnp("data: checker: payload read");
        free(buffer);
        return -1;
    }

    // update statistics
    zdb_rootsettings.stats.datadiskread += header.datalength;

    // checking integrity of the payload
    uint32_t integrity = data_crc32(buffer, header.datalength);
    free(buffer);

    zdb_debug("[+] data: checker: %08x <> %08x\n", integrity, header.integrity);

    // comparing with header
    return (integrity == header.integrity);
}

// check payload integrity from any datafile
// function wrapper to load the correct file id
int data_check(data_root_t *root, size_t offset, uint16_t dataid) {
    int fd;

    // acquire data id fd
    if((fd = data_grab_dataid(root, dataid)) < 0)
        return -1;

    int value = data_check_real(fd, offset);

    // release dataid
    data_release_dataid(root, dataid, fd);

    return value;
}



// insert data on the datafile and returns it's offset
// size_t data_insert(data_root_t *root, unsigned char *data, uint32_t datalength, void *vid, uint8_t idlength, uint8_t flags, uint32_t crc) {
size_t data_insert(data_root_t *root, data_request_t *source) {
    unsigned char *id = (unsigned char *) source->vid;
    size_t offset = lseek(root->datafd, 0, SEEK_END);
    size_t headerlength = sizeof(data_entry_header_t) + source->idlength;
    data_entry_header_t *header;

    if(!(header = malloc(headerlength)))
        zdb_diep("malloc");

    header->idlength = source->idlength;
    header->datalength = source->datalength;
    header->previous = root->previous;
    header->integrity = source->crc; // data_crc32(data, datalength);
    header->flags = source->flags;
    header->timestamp = time(NULL);

    memcpy(header->id, id, source->idlength);

    // data offset will always be >= 1 (see initializer notes)
    // we can use 0 as error detection

    if(!data_write(root->datafd, header, headerlength, 0, root)) {
        zdb_verbose("[-] data header: write failed\n");
        free(header);
        return 0;
    }

    free(header);

    if(!data_write(root->datafd, source->data, source->datalength, 1, root)) {
        zdb_verbose("[-] data payload: write failed\n");
        return 0;
    }

    // set this current offset as the latest
    // offset inserted
    root->previous = offset;

    return offset;
}

// return the offset of the next entry which will be added
// you probably don't need this, you should get the offset back
// when data is really inserted, but this could be needed, for
// exemple in direct key mode, when the key depends of the offset
// itself
size_t data_next_offset(data_root_t *root) {
    return lseek(root->datafd, 0, SEEK_END);
}

int data_entry_is_deleted(data_entry_header_t *entry) {
    return (entry->flags & DATA_ENTRY_DELETED);
}

// add a new empty entry, flagged as deleted
// with the id, so we know (when reading the datafile) that this key
// was deleted
// this is needed in order to rebuild an index from data file and
// for compaction process
int data_delete(data_root_t *root, void *id, uint8_t idlength) {
    unsigned char *empty = (unsigned char *) "";

    data_request_t dreq = {
        .data = empty,
        .datalength = 0,
        .vid = id,
        .idlength = idlength,
        .flags = DATA_ENTRY_DELETED,
        .crc = 0,
    };

    zdb_debug("[+] data: delete: insert empty flagged data\n");
    if(!(data_insert(root, &dreq)))
        return 0;

    return 1;
}

uint16_t data_dataid(data_root_t *root) {
    return root->dataid;
}

//
// data constructor and destructor
//
void data_destroy(data_root_t *root) {
    free(root->datafile);
    free(root);
}

data_root_t *data_init_lazy(zdb_settings_t *settings, char *datapath, uint16_t dataid) {
    data_root_t *root = (data_root_t *) malloc(sizeof(data_root_t));

    root->datafd = 0;
    root->datadir = datapath;
    root->datafile = malloc(sizeof(char) * (ZDB_PATH_MAX + 1));
    root->dataid = dataid;
    root->sync = settings->sync;
    root->synctime = settings->synctime;
    root->lastsync = 0;
    root->previous = 0;

    data_set_id(root);

    return root;
}

data_root_t *data_init(zdb_settings_t *settings, char *datapath, uint16_t dataid) {
    data_root_t *root = data_init_lazy(settings, datapath, dataid);

    // opening the file and creating it if needed
    data_initialize(root->datafile, root);

    // opening the final file for appending only
    data_open_final(root);

    return root;
}

void data_emergency(data_root_t *root) {
    if(!root)
        return;

    fsync(root->datafd);
}

// delete data files
void data_delete_files(data_root_t *root) {
    zdb_dir_clean_payload(root->datadir);
}
