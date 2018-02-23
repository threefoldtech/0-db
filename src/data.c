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
        debug("[+] data: last sync expired, force sync\n");
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
        warnp("data write");
        return 0;
    }

    if(response != (ssize_t) length) {
        fprintf(stderr, "[-] data write: partial write\n");
        return 0;
    }

    if(syncer)
        data_sync_check(root, fd);

    return 1;
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

        diep(filename);
    }

    // writing initial header
    data_header_t header;

    memcpy(header.magic, "DAT0", 4);
    header.version = 1;
    header.created = time(NULL);
    header.opened = 0; // not supported yet
    header.fileid = root->dataid;

    if(!data_write(fd, &header, sizeof(data_header_t), 1, root))
        diep(filename);

    close(fd);
}

// simply set globaly the current filename based on it's id
static void data_set_id(data_root_t *root) {
    sprintf(root->datafile, "%s/zdb-data-%05u", root->datadir, root->dataid);
}

// open the datafile based on it's id
static int data_open_id(data_root_t *root, uint16_t id) {
    char temp[PATH_MAX];
    int fd;

    sprintf(temp, "%s/zdb-data-%05u", root->datadir, id);

    if((fd = open(temp, O_RDONLY, 0600)) < 0) {
        warnp(temp);
        return -1;
    }

    return fd;
}

static void data_open_final(data_root_t *root) {
    // try to open the datafile in write mode to append new data
    if((root->datafd = open(root->datafile, O_CREAT | O_RDWR | O_APPEND, 0600)) < 0) {
        // maybe we are on a read-only filesystem
        // let's try to open it in read-only
        if(errno != EROFS)
            diep(root->datafile);

        if((root->datafd = open(root->datafile, O_RDONLY, 0600)) < 0)
            diep(root->datafile);

        debug("[+] data: file opened in read-only mode\n");
    }

    // skipping header
    lseek(root->datafd, 0, SEEK_END);

    printf("[+] data: active file: %s\n", root->datafile);
}

// jumping to the next id close the current data file
// and open the next id file, it will create the new file
size_t data_jump_next(data_root_t *root, uint16_t newid) {
    verbose("[+] data: jumping to the next file\n");

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
    data_entry_header_t header;

    // moving the the header offset
    lseek(fd, offset, SEEK_SET);

    if(read(fd, &header, sizeof(data_entry_header_t)) != sizeof(data_entry_header_t)) {
        warnp("data header read");
        return 0;
    }

    return header.datalength;
}

// get a payload from any datafile
data_payload_t data_get(data_root_t *root, size_t offset, size_t length, uint16_t dataid, uint8_t idlength) {
    int fd = root->datafd;
    data_payload_t payload = {
        .buffer = NULL,
        .length = 0
    };

    if(root->dataid != dataid) {
        // the requested datafile is not the current datafile opened
        // we will re-open the expected datafile temporary
        debug("[-] data: current file: %d, requested: %d, switching\n", root->dataid, dataid);
        if((fd = data_open_id(root, dataid)) < 0)
            return payload;
    }

    // if we don't know the length in advance, we will read the
    // data header to know the payload size from it
    if(length == 0) {
        debug("[+] data: fetching length from datafile\n");

        if((length = data_length_from_offset(fd, offset)) == 0)
            return payload;

        debug("[+] data: length from datafile: %zu\n", length);
    }

    // positioning datafile to expected offset
    // and skiping header (pointing to payload)
    lseek(fd, offset + sizeof(data_entry_header_t) + idlength, SEEK_SET);

    // allocating buffer from length
    // (from index or data header, we don't care)
    payload.buffer = malloc(length);
    payload.length = length;

    if(read(fd, payload.buffer, length) != (ssize_t) length) {
        warnp("data_get: read");

        free(payload.buffer);
        payload.buffer = NULL;
    }

    if(root->dataid != dataid) {
        // closing the temporary file descriptor
        // we only keep the current one
        close(fd);
    }

    return payload;
}

// get a payload from any datafile
int data_check(data_root_t *root, size_t offset, uint16_t dataid) {
    int fd = root->datafd;
    unsigned char *buffer;
    data_entry_header_t header;

    if(root->dataid != dataid) {
        // the requested datafile is not the current datafile opened
        // we will re-open the expected datafile temporary
        debug("[-] data: checker: current file: %d, requested: %d, switching\n", root->dataid, dataid);
        if((fd = data_open_id(root, dataid)) < 0)
            return -1;
    }

    // positioning datafile to expected offset
    // and skiping header (pointing to payload)
    lseek(fd, offset, SEEK_SET);

    if(read(fd, &header, sizeof(data_entry_header_t)) != (ssize_t) sizeof(data_entry_header_t)) {
        warnp("data: checker: header read");
        return -1;
    }

    // skipping the key, set buffer to payload point
    lseek(fd, header.idlength, SEEK_CUR);

    // allocating buffer from header's length
    buffer = malloc(header.datalength);

    if(read(fd, buffer, header.datalength) != (ssize_t) header.datalength) {
        warnp("data: checker: payload read");
        free(buffer);
        return -1;
    }

    if(root->dataid != dataid) {
        // closing the temporary file descriptor
        // we only keep the current one
        close(fd);
    }

    // checking integrity of the payload
    uint32_t integrity = data_crc32(buffer, header.datalength);

    debug("[+] data: checker: %08x <> %08x\n", integrity, header.integrity);

    // comparing with header
    return (integrity == header.integrity);
}


// insert data on the datafile and returns it's offset
size_t data_insert(data_root_t *root, unsigned char *data, uint32_t datalength, void *vid, uint8_t idlength) {
    unsigned char *id = (unsigned char *) vid;
    size_t offset = lseek(root->datafd, 0, SEEK_END);
    size_t headerlength = sizeof(data_entry_header_t) + idlength;
    data_entry_header_t *header;

    if(!(header = malloc(headerlength)))
        diep("malloc");

    header->idlength = idlength;
    header->datalength = datalength;
    header->integrity = data_crc32(data, datalength);

    memcpy(header->id, id, idlength);

    // data offset will always be >= 1 (see initializer notes)
    // we can use 0 as error detection

    if(!data_write(root->datafd, header, headerlength, 0, root)) {
        verbose("[-] data header: write failed\n");
        free(header);
        return 0;
    }

    free(header);

    if(!data_write(root->datafd, data, datalength, 1, root)) {
        verbose("[-] data payload: write failed\n");
        return 0;
    }

    return offset;
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

data_root_t *data_init(settings_t *settings, char *datapath, uint16_t dataid) {
    data_root_t *root = (data_root_t *) malloc(sizeof(data_root_t));

    root->datadir = datapath;
    root->datafile = malloc(sizeof(char) * (PATH_MAX + 1));
    root->dataid = dataid;
    root->sync = settings->sync;
    root->synctime = settings->synctime;
    root->lastsync = 0;

    data_set_id(root);

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
