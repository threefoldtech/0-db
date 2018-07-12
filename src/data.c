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
#include "index.h" // for key max length

// dump a data entry
static void data_entry_header_dump(data_entry_header_t *entry) {
#ifdef RELEASE
    (void) entry;
#else
    debug("[+] data: entry dump: id length  : %u\n", entry->idlength);
    debug("[+] data: entry dump: data length: %u\n", entry->datalength);
    debug("[+] data: entry dump: previous   : %u\n", entry->previous);
    debug("[+] data: entry dump: integrity  : %X\n", entry->integrity);
    debug("[+] data: entry dump: flags      : %u\n", entry->flags);
    debug("[+] data: entry dump: timestamp  : %u\n", entry->timestamp);
#endif
}

// force to sync the data buffer into the underlaying device
static inline int data_sync(data_root_t *root, int fd) {
    fsync(fd);
    root->lastsync = time(NULL);
    return 1;
}

// checking wether some sync is forced
// there are two possibilities:
// - we set --sync option on runtime, then each write is sync forced
// - we set --synctime on runtime and after this period (in seconds)
//   we force to sync the last write
static inline int data_sync_check(data_root_t *root, int fd) {
    if(root->sync)
        return data_sync(root, fd);

    if(!root->synctime)
        return 0;

    if((time(NULL) - root->lastsync) > root->synctime) {
        debug("[+] data: last sync expired, forcing sync\n");
        return data_sync(root, fd);
    }

    return 0;
}

// wrap (mostly) all write operations to the datafile.
// It's easier to keep a single logic with error handling
// related to write checking
// This function takes an extra argument "syncer" which explicitly
// ask to check if we need to do some sync-check or not.
// This is useful when writing data, we write the header and then the payload
// so we can avoid to do two sync calls (only one when done)
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

// open one datafile based on it's id
// in case of error, the reason will be printed and -1 will be returned
// otherwise the file descriptor is returned
//
// this function opens the data file in read only mode and should
// not be used to edit or open the effective current file.
static int data_open_id_mode(data_root_t *root, uint16_t id, int mode) {
    char temp[PATH_MAX];
    int fd;

    sprintf(temp, "%s/zdb-data-%05u", root->datadir, id);

    if((fd = open(temp, mode, 0600)) < 0) {
        warnp(temp);
        return -1;
    }

    return fd;
}

// default mode, read-only datafile
static int data_open_id(data_root_t *root, uint16_t id) {
    return data_open_id_mode(root, id, O_RDONLY);
}

// special case (for deletion) where read-write is needed
// and not in append mode
int data_get_dataid_rw(data_root_t *root, uint16_t id) {
    return data_open_id_mode(root, id, O_RDWR);
}


// main function to call when you need to deal with data id.
// This function takes care of opening the right file id:
//  - if you want the current opened file id, you have the fd
//  - if the file is not opened yet, you'll receive a new fd
// you need to call the .... to be consistent about cleaning this
// file open, if a new one was opened
//
// if the data id could not be opened, -1 is returned
static inline int data_grab_dataid(data_root_t *root, uint16_t dataid) {
    int fd = root->datafd;

    if(root->dataid != dataid) {
        // the requested datafile is not the current datafile opened
        // we will re-open the expected datafile temporarily
        debug("[-] data: switching file: %d, requested: %d\n", root->dataid, dataid);
        if((fd = data_open_id(root, dataid)) < 0)
            return -1;
    }

    return fd;
}

static inline void data_release_dataid(data_root_t *root, uint16_t dataid, int fd) {
    // if the requested data id (or fd) is not the one
    // currently in use by the main structure, we close it
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

        diep(filename); // shouldn't we have some BIG message here ?
    }

    // writing initial header
    data_header_t header;

    memcpy(header.magic, "DAT0", 4);
    header.version = ZDB_DATAFILE_VERSION;
    header.created = time(NULL);
    header.opened = 0; // not supported yet
    header.fileid = root->dataid;

    if(!data_write(fd, &header, sizeof(data_header_t), 1, root))
        diep(filename);

    close(fd);
}

// simply set globally the current filename based on it's id
static void data_set_id(data_root_t *root) {
    sprintf(root->datafile, "%s/zdb-data-%05u", root->datadir, root->dataid);
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

    // jumping to the first entry
    lseek(root->datafd, sizeof(data_header_t), SEEK_SET);

    // reading all indexes to find where is the last one
    data_entry_header_t header;
    int entries = 0;

    debug("[+] data: reading file, finding last entry\n");

    while(read(root->datafd, &header, sizeof(data_entry_header_t)) == sizeof(data_entry_header_t)) {
        root->previous = lseek(root->datafd, 0, SEEK_CUR) - sizeof(data_entry_header_t);
        lseek(root->datafd, header.datalength + header.idlength, SEEK_CUR);

        entries += 1;
    }

    debug("[+] data: entries read: %d, last offset: %lu\n", entries, root->previous);
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
// this function uses Intel CRC32 (SSE4.2) intrinsic (SIMD)
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

    // moving to the header offset
    lseek(fd, offset, SEEK_SET);

    if(read(fd, &header, sizeof(data_entry_header_t)) != sizeof(data_entry_header_t)) {
        warnp("incorrect data header read");
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

    // if we don't know the length in advance, so we read the
    // data header to know the payload size 
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
        warnp("data_get: incorrect read length");

        free(payload.buffer);
        payload.buffer = NULL;
    }

    return payload;
}

// wrapper for data_get_real, which opens the right dataid
// allowing to do only what's necessary and this wrapper just prepares the right data id
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

    // positioning to the expected offset in the datafile
    lseek(fd, offset, SEEK_SET);

    if(read(fd, &header, sizeof(data_entry_header_t)) != (ssize_t) sizeof(data_entry_header_t)) {
        warnp("data: checker: header read");
        return -1;
    }

    // skipping the key, set buffer to payload position
    lseek(fd, header.idlength, SEEK_CUR);

    // allocating buffer from header's length
    buffer = malloc(header.datalength);

    if(read(fd, buffer, header.datalength) != (ssize_t) header.datalength) {
        warnp("data: checker: payload read");
        free(buffer);
        return -1;
    }

    // checking integrity of the payload
    uint32_t integrity = data_crc32(buffer, header.datalength);
    free(buffer);

    debug("[+] data: checker: %08x <> %08x\n", integrity, header.integrity);

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



// insert data to the datafile and return it's offset
size_t data_insert(data_root_t *root, unsigned char *data, uint32_t datalength, void *vid, uint8_t idlength) {
    unsigned char *id = (unsigned char *) vid;
    size_t offset = lseek(root->datafd, 0, SEEK_END);
    size_t headerlength = sizeof(data_entry_header_t) + idlength;
    data_entry_header_t *header;

    if(!(header = malloc(headerlength)))
        diep("Could not allocate memory! (data_insert)");

    header->idlength = idlength;
    header->datalength = datalength;
    header->previous = root->previous;
    header->integrity = data_crc32(data, datalength);
    header->flags = 0;
    header->timestamp = time(NULL);

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

#if 0
// this function will check for a legitime request inside the data set
// to estimate if a request is legitimate, we assume that
//  - if the offset provided point to a header
//  - we can't ensure what we read is, for sure, a header
//  - to improve probability:
//    - if the length of the key in the header match expected key length
//    - if the data length is not more than the maximum allowed size
//    - if the key in the header match the key requested
//    if all of theses conditions match, the probability of a fake request
//    are nearly null
//
// if everything is good, returns the datalength from the header, 0 otherwise
static inline size_t data_match_real(int fd, void *id, uint8_t idlength, size_t offset) {
    data_entry_header_t header;
    char keycheck[MAX_KEY_LENGTH];

    // positioning datafile to expected offset
    lseek(fd, offset, SEEK_SET);

    // reading the header
    if(read(fd, &header, sizeof(data_entry_header_t)) != (ssize_t) sizeof(data_entry_header_t)) {
        warnp("data: validator: header read");
        return 0;
    }

    // preliminary check: does key length match
    if(header.idlength != idlength) {
        debug("[-] data: validator: key-length mismatch\n");
        return 0;
    }

    if(header.flags & DATA_ENTRY_DELETED) {
        debug("[-] data: validator: entry deleted\n");
        return 0;
    }

    // preliminary check: does the payload fit on the file
    if(header.datalength > DATA_MAXSIZE) {
        debug("[-] data: validator: payload length too big\n");
        return 0;
    }

    // comparing the key
    if(read(fd, keycheck, idlength) != (ssize_t) idlength) {
        warnp("data: validator: key read");
        return 0;
    }

    if(memcmp(keycheck, id, idlength) != 0) {
        debug("[-] data: validator: key mismatch\n");
        return 0;
    }

    return header.datalength;
}

// wrapper for data_match_real which load the correct file id
// this function is made to ensure the key requested is legitimate
// we need to be careful, we cannot trust anything (file id, offset, ...)
//
// if the header matchs, returns the datalength, which is mostly the only
// missing data we have in direct-key mode
size_t data_match(data_root_t *root, void *id, uint8_t idlength, size_t offset, uint16_t dataid) {
    int fd;

    // acquire data id fd
    if((fd = data_grab_dataid(root, dataid)) < 0) {
        debug("[-] data: validator: could not open requested file id (%u)\n", dataid);
        return 0;
    }

    size_t length = data_match_real(fd, id, idlength, offset);

    // release dataid
    data_release_dataid(root, dataid, fd);

    return length;
}
#endif

int data_delete_real(int fd, size_t offset) {
    data_entry_header_t header;

    // blindly move to the offset
    lseek(fd, offset, SEEK_SET);

    // read current header
    if(read(fd, &header, sizeof(data_entry_header_t)) != (ssize_t) sizeof(data_entry_header_t)) {
        warnp("data: delete: unmatched header read length (data_delete_real)");
        return 0;
    }

    // flag entry as deleted
    header.flags |= DATA_ENTRY_DELETED;

    // rollback to the offset
    lseek(fd, offset, SEEK_SET);

    // overwrite the header with the new flag
    if(write(fd, &header, sizeof(data_entry_header_t)) != (ssize_t) sizeof(data_entry_header_t)) {
        warnp("data: delete: unmatched header write length (data_delete_real)");
        return 0;
    }

    return 1;
}

// IMPORTANT:
//   this function is the only one to 'break' the always append
//   behavior, this function will overwrite an existing index entry by
//   seeking and rewrite the header
//
// when deleting some data, we mark (flag) this data as deleted which
// allows two things:
//   - we can do compaction offline by removing theses blocks
//   - we still can rebuild an index based on the datafile only
//
// during runtime, this flag will be checked only using the data_match
// function
int data_delete(data_root_t *root, size_t offset, uint16_t dataid) {
    int fd;

    debug("[+] data: delete: opening datafile in read-write mode\n");

    // acquire data id fd
    if((fd = data_get_dataid_rw(root, dataid)) < 0) {
        debug("[-] data: delete: could not open requested file id (%u) (data_delete)\n", dataid);
        return 0;
    }

    int value = data_delete_real(fd, offset);

    // release dataid
    close(fd);

    return value;
}

uint16_t data_dataid(data_root_t *root) {
    return root->dataid;
}

//
// walk functions
//
static inline data_scan_t data_scan_error(data_scan_t original, data_scan_status_t error) {
    // clean any remaning memory
    free(original.header);

    // ensure we reset everything
    original.header = NULL;
    original.status = error;

    return original;
}

// RSCAN implementation
static data_scan_t data_previous_header_real(data_scan_t scan) {
    data_entry_header_t source;

    // if scan.target is not set yet, we don't know the expected
    // offset of the previous header, let's read the header
    // of the current entry and find out at what position is the next one
    if(scan.target == 0) {
        off_t current = lseek(scan.fd, scan.original, SEEK_SET);

        if(read(scan.fd, &source, sizeof(data_entry_header_t)) != sizeof(data_entry_header_t)) {
            warnp("data: previous-header: could not read original offset datafile");
            return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
        }

        scan.target = source.previous;

        if(source.previous > current) {
            debug("[+] data: previous-header: previous offset in previous file\n");
            return data_scan_error(scan, DATA_SCAN_REQUEST_PREVIOUS);
        }
    }

    debug("[+] data: previous-header: offset: %u\n", source.previous);

    // at this point, we know scan.target is set to the expected value
    if(scan.target == 0) {
        debug("[+] data: previous-header: zero reached, nothing to rollback\n");
        return data_scan_error(scan, DATA_SCAN_NO_MORE_DATA);
    }

    // jumping to previous object
    lseek(scan.fd, scan.target, SEEK_SET);

    // reading the fixed-length
    if(read(scan.fd, &source, sizeof(data_entry_header_t)) != sizeof(data_entry_header_t)) {
        warnp("data: previous-header: could not read previous offset datafile");
        return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
    }

    // checking if entry is deleted
    if(source.flags & DATA_ENTRY_DELETED) {
        debug("[+] data: previous-header: data is deleted, going one further\n");

        // set the 'new' original to this offset
        scan.original = scan.target;

        // reset target, so next time we come here, we will refetch previous
        // entry and use the mechanism to check if it's the previous file and
        // so on
        scan.target = 0;

        // let's notify source this entry was deleted and we
        // should retrigger the fetch
        return data_scan_error(scan, DATA_SCAN_DELETED);
    }

    if(!(scan.header = (data_entry_header_t *) malloc(sizeof(data_entry_header_t) + source.idlength))) {
        warnp("data: previous-header: malloc");
        return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        warnp("data: previous-header: could not read id from datafile");
        return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
    }

    debug("[+] data: previous-header: entry found\n");
    scan.status = DATA_SCAN_SUCCESS;

    return scan;
}

data_scan_t data_previous_header(data_root_t *root, uint16_t dataid, size_t offset) {
    data_scan_t scan = {
        .fd = 0,
        .original = offset, // offset of the 'current' key
        .target = 0,        // offset of the 'previous' key
        .header = NULL,     // the previous header
        .status = DATA_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire data id fd
        if((scan.fd = data_grab_dataid(root, dataid)) < 0) {
            debug("[-] data: previous-header: could not open requested file id (%u)\n", dataid);
            return data_scan_error(scan, DATA_SCAN_NO_MORE_DATA);
        }

        // trying to get entry
        scan = data_previous_header_real(scan);

        // release dataid
        data_release_dataid(root, dataid, scan.fd);

        if(scan.status == DATA_SCAN_SUCCESS)
            return scan;

        if(scan.status == DATA_SCAN_UNEXPECTED)
            return scan;

        if(scan.status == DATA_SCAN_NO_MORE_DATA)
            return scan;

        // entry was deleted, scan object is updated
        // we need to retry fetching new data
        if(scan.status == DATA_SCAN_DELETED)
            continue;

        if(scan.status == DATA_SCAN_REQUEST_PREVIOUS)
            dataid -= 1;
    }

    // never reached
}

// SCAN implementation
static data_scan_t data_next_header_real(data_scan_t scan) {
    data_entry_header_t source;

    // if scan.target is not set yet, we don't know the expected
    // offset of the next header, let's read the header
    // of the current entry and find out which is the next one
    if(scan.target == 0) {
        lseek(scan.fd, scan.original, SEEK_SET);

        if(read(scan.fd, &source, sizeof(data_entry_header_t)) != sizeof(data_entry_header_t)) {
            warnp("data: next-header: could not read original offset datafile");
            return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
        }

        debug("[+] data: next-header: this length: %u\n", source.datalength);

        // next header is at offset + this header + payload
        scan.target = scan.original + sizeof(data_entry_header_t);
        scan.target += source.idlength + source.datalength;
    }

    // jumping to next object
    lseek(scan.fd, scan.target, SEEK_SET);

    // reading the fixed-length
    if(read(scan.fd, &source, sizeof(data_entry_header_t)) != sizeof(data_entry_header_t)) {
        warnp("data: next-header: could not read next offset datafile");
        // this mean the data expected is the first of the next datafile
        scan.target = sizeof(data_header_t);
        return data_scan_error(scan, DATA_SCAN_EOF_REACHED);
    }

    // checking if entry is deleted
    if(source.flags & DATA_ENTRY_DELETED) {
        debug("[+] data: next-header: data is deleted, going one further\n");

        // set the 'new' original to this offset
        scan.original = scan.target;

        // reset target, so next time we come here, we will refetch previous
        // entry and use the mechanism to check if it's the previous file and
        // so on
        scan.target = 0;

        // let's notify source this entry was deleted and we
        // should retrigger the fetch
        return data_scan_error(scan, DATA_SCAN_DELETED);
    }


    if(!(scan.header = (data_entry_header_t *) malloc(sizeof(data_entry_header_t) + source.idlength))) {
        warnp("data: next-header: malloc");
        return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        warnp("data: next-header: could not read id from datafile");
        return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
    }

    debug("[+] data: next-header: entry found\n");
    scan.status = DATA_SCAN_SUCCESS;

    return scan;
}

data_scan_t data_next_header(data_root_t *root, uint16_t dataid, size_t offset) {
    data_scan_t scan = {
        .fd = 0,
        .original = offset, // offset of the 'current' key
        .target = 0,        // offset of the expected next header
        .header = NULL,     // the new header
        .status = DATA_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire data id fd
        if((scan.fd = data_grab_dataid(root, dataid)) < 0) {
            debug("[-] data: next-header: could not open requested file id (%u)\n", dataid);
            return data_scan_error(scan, DATA_SCAN_NO_MORE_DATA);
        }

        // trying to get entry
        scan = data_next_header_real(scan);

        // release dataid
        data_release_dataid(root, dataid, scan.fd);

        if(scan.status == DATA_SCAN_SUCCESS)
            return scan;

        if(scan.status == DATA_SCAN_UNEXPECTED)
            return scan;

        // entry was deleted, scan object is updated
        // we need to retry fetching new data
        if(scan.status == DATA_SCAN_DELETED)
            continue;

        if(scan.status == DATA_SCAN_EOF_REACHED) {
            debug("[-] data: next-header: eof reached\n");
            dataid += 1;
        }
    }

    // never reached
}

static data_scan_t data_first_header_real(data_scan_t scan) {
    data_entry_header_t source;

    // jumping to next object
    lseek(scan.fd, scan.target, SEEK_SET);

    // reading the fixed-length 
    if(read(scan.fd, &source, sizeof(data_entry_header_t)) != sizeof(data_entry_header_t)) {
        warnp("data: first-header: could not read next offset datafile");
        // this means that the expected data is the first of the next datafile
        scan.target = sizeof(data_header_t);
        return data_scan_error(scan, DATA_SCAN_EOF_REACHED);
    }

    // checking if entry is deleted
    if(source.flags & DATA_ENTRY_DELETED) {
        debug("[+] data: first-header: data is deleted, going one further\n");

        // jump to the next entry
        scan.target += sizeof(data_entry_header_t);
        scan.target += source.idlength + source.datalength;

        // let's notify source this entry was deleted and we
        // should retrigger the fetch
        return data_scan_error(scan, DATA_SCAN_DELETED);
    }

    if(!(scan.header = (data_entry_header_t *) malloc(sizeof(data_entry_header_t) + source.idlength))) {
        warnp("data: first-header: malloc");
        return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        warnp("data: first-header: could not read id from datafile");
        return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
    }

    debug("[+] data: first-header: entry found\n");
    scan.status = DATA_SCAN_SUCCESS;

    return scan;
}


data_scan_t data_first_header(data_root_t *root) {
    uint16_t dataid = 0;
    data_scan_t scan = {
        .fd = 0,
        .original = sizeof(data_header_t), // offset of the first key
        .target = sizeof(data_header_t),   // again offset of the first key
        .header = NULL,
        .status = DATA_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire data id fd
        if((scan.fd = data_grab_dataid(root, dataid)) < 0) {
            debug("[-] data: first-header: could not open requested file id (%u)\n", dataid);
            return data_scan_error(scan, DATA_SCAN_NO_MORE_DATA);
        }

        scan = data_first_header_real(scan);

        // release dataid
        data_release_dataid(root, dataid, scan.fd);

        if(scan.status == DATA_SCAN_SUCCESS)
            return scan;

        if(scan.status == DATA_SCAN_UNEXPECTED)
            return scan;

        // entry was deleted, scan object is updated
        // we need to retry fetching new data
        if(scan.status == DATA_SCAN_DELETED)
            continue;

        if(scan.status == DATA_SCAN_EOF_REACHED) {
            debug("[-] data: next-header: eof reached\n");
            dataid += 1;
        }
    }

    // never reached
}

static data_scan_t data_last_header_real(data_scan_t scan) {
    data_entry_header_t source;

    debug("[+] data: last-header: trying previous offset: %lu\n", scan.target);

    // at that point, we know scan.target is set to the expected value
    if(scan.target == 0) {
        debug("[+] data: last-header: zero reached, nothing to rollback\n");
        return data_scan_error(scan, DATA_SCAN_NO_MORE_DATA);
    }

    // jumping to previous object
    lseek(scan.fd, scan.target, SEEK_SET);

    // reading the fixed-length
    if(read(scan.fd, &source, sizeof(data_entry_header_t)) != sizeof(data_entry_header_t)) {
        warnp("data: previous-header: could not read previous offset datafile");
        return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
    }

    data_entry_header_dump(&source);

    // checking if entry is deleted
    if(source.flags & DATA_ENTRY_DELETED) {
        debug("[+] data: last-header: data is deleted, going one further\n");

        // reset target, so next time we come here, we will refetch previous
        // entry and use the mechanism to check if it's the previous file and
        // so on
        scan.target = source.previous;

        // let's notify source this entry was deleted and we
        // should retrigger the fetch
        return data_scan_error(scan, DATA_SCAN_DELETED);
    }

    if(!(scan.header = (data_entry_header_t *) malloc(sizeof(data_entry_header_t) + source.idlength))) {
        warnp("data: last-header: malloc");
        return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        warnp("data: last-header: could not read id from datafile");
        return data_scan_error(scan, DATA_SCAN_UNEXPECTED);
    }

    debug("[+] data: last-header: entry found\n");
    scan.status = DATA_SCAN_SUCCESS;

    return scan;
}


data_scan_t data_last_header(data_root_t *root) {
    uint16_t dataid = root->dataid;
    data_scan_t scan = {
        .fd = 0,
        .original = root->previous, // offset of the last key
        .target = root->previous,   // again offset of the last key
        .header = NULL,
        .status = DATA_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire data id fd
        if((scan.fd = data_grab_dataid(root, dataid)) < 0) {
            debug("[-] data: last-header: could not open requested file id (%u)\n", dataid);
            return data_scan_error(scan, DATA_SCAN_NO_MORE_DATA);
        }

        // trying to get entry
        scan = data_last_header_real(scan);

        // release dataid
        data_release_dataid(root, dataid, scan.fd);

        if(scan.status == DATA_SCAN_SUCCESS)
            return scan;

        if(scan.status == DATA_SCAN_UNEXPECTED)
            return scan;

        if(scan.status == DATA_SCAN_NO_MORE_DATA)
            return scan;

        // entry was deleted, scan object is updated
        // we need to retry fetching new data
        if(scan.status == DATA_SCAN_DELETED)
            continue;

        if(scan.status == DATA_SCAN_REQUEST_PREVIOUS)
            dataid -= 1;
    }

    // never reached
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
    root->previous = 0;

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
