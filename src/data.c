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
#include "zerodb.h"
#include "data.h"

// global pointer of the main data object
// we are single-threaded by design and this is mostly
// only changed during initializing and file-swap
static data_t *rootdata = NULL;

// force to sync data buffer into the underlaying device
static inline int data_sync(int fd) {
    fsync(fd);
    rootdata->lastsync = time(NULL);
    return 1;
}

// checking is some sync is forced
// there is two possibilities:
// - we set --sync option on runtime, and each write is sync forced
// - we set --synctime on runtime and after this amount of seconds
//   we force to sync the last write
static inline int data_sync_check(int fd) {
    if(rootdata->sync)
        return data_sync(fd);

    if(!rootdata->synctime)
        return 0;

    if((time(NULL) - rootdata->lastsync) > rootdata->synctime) {
        debug("[+] data: last sync expired, force sync\n");
        return data_sync(fd);
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
static int data_write(int fd, void *buffer, size_t length, int syncer) {
    ssize_t response;

    if((response = write(fd, buffer, length)) < 0) {
        warnp("data write");
        return 0;
    }

    if(response != (ssize_t) length) {
        fprintf(stderr, "[-] data write: partial write\n");
        return 0;
    }

    if(syncer)
        data_sync_check(fd);

    return 1;
}

//
// data management
//
void data_initialize(char *filename) {
    int fd;

    if((fd = open(filename, O_CREAT | O_RDWR, 0600)) < 0) {
        // ignoring initializer on read-only filesystem
        if(errno == EROFS)
            return;

        diep(filename);
    }

    // writing initial header
    // this header is, right now, not used and not useful
    // we should probably improve it
    //
    // anyway, it's important for the implementation to have
    // at least 1 byte already written, we use 0 as error when
    // expecting an offset
    if(!data_write(fd, "X", 1, 1))
        diep(filename);

    close(fd);
}

// simply set globaly the current filename based on it's id
static void data_set_id(data_t *root) {
    sprintf(root->datafile, "%s/zdb-data-%05u", root->datadir, root->dataid);
}

// open the datafile based on it's id
static int data_open_id(data_t *root, uint16_t id) {
    char temp[PATH_MAX];
    int fd;

    sprintf(temp, "%s/zdb-data-%05u", root->datadir, id);

    if((fd = open(temp, O_RDONLY, 0600)) < 0)
        diep(temp);

    return fd;
}

static void data_open_final(data_t *root) {
    // try to open the datafile in write mode to append new data
    if((root->datafd = open(root->datafile, O_CREAT | O_RDWR | O_APPEND, 0600)) < 0) {
        // maybe we are on a read-only filesystem
        // let's try to open it in read-only
        if(errno != EROFS)
            diep(root->datafile);

        if((root->datafd = open(root->datafile, O_RDONLY, 0600)) < 0)
            diep(root->datafile);

        debug("[+] data file opened in read-only mode\n");
    }

    // skipping header
    lseek(root->datafd, 0, SEEK_END);

    printf("[+] active data file: %s\n", root->datafile);
}

// jumping to the next id close the current data file
// and open the next id file, it will create the new file
size_t data_jump_next() {
    verbose("[+] jumping to the next data file\n");

    // closing current file descriptor
    close(rootdata->datafd);

    // moving to the next file
    rootdata->dataid += 1;
    data_set_id(rootdata);

    data_initialize(rootdata->datafile);
    data_open_final(rootdata);

    return rootdata->dataid;
}

// compute a crc32 of the payload
// this function uses Intel CRC32 (SSE4.2) intrinsic
static uint32_t data_crc32(const uint8_t *bytes, ssize_t length) {
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
    data_header_t header;

    // moving the the header offset
    lseek(fd, offset, SEEK_SET);

    if(read(fd, &header, sizeof(data_header_t)) != sizeof(data_header_t)) {
        warnp("data header read");
        return 0;
    }

    return header.datalength;
}

// get a payload from any datafile
data_payload_t data_get(size_t offset, size_t length, uint16_t dataid, uint8_t idlength) {
    int fd = rootdata->datafd;
    data_payload_t payload = {
        .buffer = NULL,
        .length = 0
    };

    if(rootdata->dataid != dataid) {
        // the requested datafile is not the current datafile opened
        // we will re-open the expected datafile temporary
        debug("[-] current data file: %d, requested: %d, switching\n", rootdata->dataid, dataid);
        fd = data_open_id(rootdata, dataid);
    }

    // if we don't know the length in advance, we will read the
    // data header to know the payload size from it
    if(length == 0) {
        debug("[+] fetching length from datafile\n");

        if((length = data_length_from_offset(fd, offset)) == 0)
            return payload;

        debug("[+] length from datafile: %zu\n", length);
    }

    // positioning datafile to expected offset
    // and skiping header (pointing to payload)
    lseek(fd, offset + sizeof(data_header_t) + idlength, SEEK_SET);

    // allocating buffer from length
    // (from index or data header, we don't care)
    payload.buffer = malloc(length);
    payload.length = length;

    if(read(fd, payload.buffer, length) != (ssize_t) length) {
        warnp("data_get: read");

        free(payload.buffer);
        payload.buffer = NULL;
    }

    if(rootdata->dataid != dataid) {
        // closing the temporary file descriptor
        // we only keep the current one
        close(fd);
    }

    return payload;
}

// insert data on the datafile and returns it's offset
size_t data_insert(unsigned char *data, uint32_t datalength, void *vid, uint8_t idlength) {
    unsigned char *id = (unsigned char *) vid;
    size_t offset = lseek(rootdata->datafd, 0, SEEK_END);
    size_t headerlength = sizeof(data_header_t) + idlength;
    data_header_t *header;

    if(!(header = malloc(headerlength)))
        diep("malloc");

    header->idlength = idlength;
    header->datalength = datalength;
    header->integrity = data_crc32(data, datalength);

    memcpy(header->id, id, idlength);

    // data offset will always be >= 1 (see initializer notes)
    // we can use 0 as error detection

    if(!data_write(rootdata->datafd, header, headerlength, 0)) {
        verbose("[-] data header: write failed\n");
        free(header);
        return 0;
    }

    free(header);

    if(!data_write(rootdata->datafd, data, datalength, 1)) {
        verbose("[-] data payload: write failed\n");
        return 0;
    }

    return offset;
}

uint16_t data_dataid() {
    return rootdata->dataid;
}

//
// data constructor and destructor
//
void data_destroy() {
    free(rootdata->datafile);
    free(rootdata);
}

void data_init(uint16_t dataid, settings_t *settings) {
    data_t *lroot = (data_t *) malloc(sizeof(data_t));

    lroot->datadir = settings->datapath;
    lroot->datafile = malloc(sizeof(char) * (PATH_MAX + 1));
    lroot->dataid = dataid;
    lroot->sync = settings->sync;
    lroot->synctime = settings->synctime;
    lroot->lastsync = 0;

    // commit variable
    rootdata = lroot;

    data_set_id(lroot);

    // opening the file and creating it if needed
    data_initialize(lroot->datafile);

    // opening the final file for appending only
    data_open_final(lroot);

}

void data_emergency() {
    fsync(rootdata->datafd);
}
