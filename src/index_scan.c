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
#include "zerodb.h"
#include "index.h"
#include "index_scan.h"

//
// walk functions
//
static inline index_scan_t index_scan_error(index_scan_t original, index_scan_status_t error) {
    // clean any remaning memory
    free(original.header);

    // ensure we reset everything
    original.header = NULL;
    original.status = error;

    return original;
}

// RSCAN implementation
static index_scan_t index_previous_header_real(index_scan_t scan) {
    index_item_t source;

    // if scan.target is not set yet, we don't know the expected
    // offset of the previous header, let's read the header
    // of the current entry and find out which is the next one
    if(scan.target == 0) {
        off_t current = lseek(scan.fd, scan.original, SEEK_SET);

        if(read(scan.fd, &source, sizeof(index_item_t)) != sizeof(index_item_t)) {
            warnp("index scan: previous-header: could not read original offset file");
            return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
        }

        scan.target = source.previous;

        if(source.previous > current) {
            debug("[+] index scan: previous-header: previous offset in previous file\n");
            return index_scan_error(scan, INDEX_SCAN_REQUEST_PREVIOUS);
        }
    }

    debug("[+] index scan: previous-header: offset: %u\n", source.previous);

    // at that point, we know scan.target is set to the expected value
    if(scan.target == 0) {
        debug("[+] index scan: previous-header: zero reached, nothing to rollback\n");
        return index_scan_error(scan, INDEX_SCAN_NO_MORE_DATA);
    }

    // jumping to previous object
    lseek(scan.fd, scan.target, SEEK_SET);

    // reading the fixed-length
    if(read(scan.fd, &source, sizeof(index_item_t)) != sizeof(index_item_t)) {
        warnp("index scan: previous-header: could not read previous offset datafile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    // checking if entry is deleted
    if(source.flags & INDEX_ENTRY_DELETED) {
        debug("[+] index scan: previous-header: data is deleted, going one more before\n");

        // set the 'new' original to this offset
        scan.original = scan.target;

        // reset target, so next time we come here, we will refetch previous
        // entry and use the mechanism to check if it's the previous file and
        // so on
        scan.target = 0;

        // let's notify source this entry was deleted and we
        // should retrigger the fetch
        return index_scan_error(scan, INDEX_SCAN_DELETED);
    }

    if(!(scan.header = (index_item_t *) malloc(sizeof(index_item_t) + source.idlength))) {
        warnp("data: previous-header: malloc");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        warnp("data: previous-header: could not read id from datafile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    debug("[+] data: previous-header: entry found\n");
    scan.status = INDEX_SCAN_SUCCESS;

    return scan;
}

index_scan_t index_previous_header(index_root_t *root, uint16_t fileid, size_t offset) {
    index_scan_t scan = {
        .fd = 0,
        .original = offset, // offset of the 'current' key
        .target = 0,        // offset of the 'previous' key
        .header = NULL,     // the previous header
        .status = INDEX_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire file id fd
        if((scan.fd = index_grab_fileid(root, fileid)) < 0) {
            debug("[-] index scan: previous-header: could not open requested file id (%u)\n", fileid);
            return index_scan_error(scan, INDEX_SCAN_NO_MORE_DATA);
        }

        // trying to get entry
        scan = index_previous_header_real(scan);

        // release fileid
        index_release_fileid(root, fileid, scan.fd);

        if(scan.status == INDEX_SCAN_SUCCESS)
            return scan;

        if(scan.status == INDEX_SCAN_UNEXPECTED)
            return scan;

        if(scan.status == INDEX_SCAN_NO_MORE_DATA)
            return scan;

        // entry was deleted, scan object is updated
        // we need to retry fetching new data
        if(scan.status == INDEX_SCAN_DELETED)
            continue;

        if(scan.status == INDEX_SCAN_REQUEST_PREVIOUS)
            fileid -= 1;
    }

    // never reached
}

// SCAN implementation
static index_scan_t index_next_header_real(index_scan_t scan) {
    index_item_t source;

    // if scan.target is not set yet, we don't know the expected
    // offset of the next header, let's read the header
    // of the current entry and find out which is the next one
    if(scan.target == 0) {
        lseek(scan.fd, scan.original, SEEK_SET);

        if(read(scan.fd, &source, sizeof(index_item_t)) != sizeof(index_item_t)) {
            warnp("index scan: next-header: could not read original offset indexfile");
            return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
        }

        debug("[+] index scan: next-header: this id length: %u\n", source.idlength);

        // next header is at offset + this header + payload
        scan.target = scan.original + sizeof(index_item_t);
        scan.target += source.idlength;
    }

    // jumping to next object
    lseek(scan.fd, scan.target, SEEK_SET);

    // reading the fixed-length
    if(read(scan.fd, &source, sizeof(index_item_t)) != sizeof(index_item_t)) {
        warnp("index scan: next-header: could not read next offset indexfile");
        // this mean the entry expected is the first of the next indexfile
        scan.target = sizeof(index_header_t);
        return index_scan_error(scan, INDEX_SCAN_EOF_REACHED);
    }

    // checking if entry is deleted
    if(source.flags & INDEX_ENTRY_DELETED) {
        debug("[+] index scan: next-header: data is deleted, going one further\n");

        // set the 'new' original to this offset
        scan.original = scan.target;

        // reset target, so next time we come here, we will refetch previous
        // entry and use the mechanism to check if it's the previous file and
        // so on
        scan.target = 0;

        // let's notify source this entry was deleted and we
        // should retrigger the fetch
        return index_scan_error(scan, INDEX_SCAN_DELETED);
    }


    if(!(scan.header = (index_item_t *) malloc(sizeof(index_item_t) + source.idlength))) {
        warnp("index scan: next-header: malloc");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        warnp("index scan: next-header: could not read id from datafile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    debug("[+] index scan: next-header: entry found\n");
    scan.status = INDEX_SCAN_SUCCESS;

    return scan;
}

index_scan_t index_next_header(index_root_t *root, uint16_t fileid, size_t offset) {
    index_scan_t scan = {
        .fd = 0,
        .original = offset, // offset of the 'current' key
        .target = 0,        // offset of the expected next header
        .header = NULL,     // the new header
        .status = INDEX_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire file id fd
        if((scan.fd = index_grab_fileid(root, fileid)) < 0) {
            debug("[-] index scan: next-header: could not open requested file id (%u)\n", fileid);
            return index_scan_error(scan, INDEX_SCAN_NO_MORE_DATA);
        }

        // trying to get entry
        scan = index_next_header_real(scan);

        // release fileid
        index_release_fileid(root, fileid, scan.fd);

        if(scan.status == INDEX_SCAN_SUCCESS)
            return scan;

        if(scan.status == INDEX_SCAN_UNEXPECTED)
            return scan;

        // entry was deleted, scan object is updated
        // we need to retry fetching new data
        if(scan.status == INDEX_SCAN_DELETED)
            continue;

        if(scan.status == INDEX_SCAN_EOF_REACHED) {
            debug("[-] index scan: next-header: eof reached\n");
            fileid += 1;
        }
    }

    // never reached
}

static index_scan_t index_first_header_real(index_scan_t scan) {
    index_item_t source;

    // jumping to next object
    lseek(scan.fd, scan.target, SEEK_SET);

    // reading the fixed-length
    if(read(scan.fd, &source, sizeof(index_item_t)) != sizeof(index_item_t)) {
        warnp("data: first-header: could not read next offset datafile");
        // this mean the data expected is the first of the next datafile
        scan.target = sizeof(index_header_t);
        return index_scan_error(scan, INDEX_SCAN_EOF_REACHED);
    }

    index_item_header_dump(&source);

    // checking if entry is deleted
    if(source.flags & INDEX_ENTRY_DELETED) {
        debug("[+] data: first-header: data is deleted, going one further\n");

        // jump to the next entry
        scan.target += sizeof(index_item_t);
        scan.target += source.idlength;

        // let's notify source this entry was deleted and we
        // should retrigger the fetch
        return index_scan_error(scan, INDEX_SCAN_DELETED);
    }

    if(!(scan.header = (index_item_t *) malloc(sizeof(index_item_t) + source.idlength))) {
        warnp("index scan: first-header: malloc");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        warnp("index scan: first-header: could not read id from datafile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    debug("[+] index scan: first-header: entry found\n");
    scan.status = INDEX_SCAN_SUCCESS;

    return scan;
}


index_scan_t index_first_header(index_root_t *root) {
    uint16_t fileid = 0;
    index_scan_t scan = {
        .fd = 0,
        .original = sizeof(index_header_t), // offset of the first key
        .target = sizeof(index_header_t),   // again offset of the first key
        .header = NULL,
        .status = INDEX_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire data id fd
        if((scan.fd = index_grab_fileid(root, fileid)) < 0) {
            debug("[-] index scan: first-header: could not open requested file id (%u)\n", fileid);
            return index_scan_error(scan, INDEX_SCAN_NO_MORE_DATA);
        }

        scan = index_first_header_real(scan);

        // release fileid
        index_release_fileid(root, fileid, scan.fd);

        if(scan.status == INDEX_SCAN_SUCCESS)
            return scan;

        if(scan.status == INDEX_SCAN_UNEXPECTED)
            return scan;

        // entry was deleted, scan object is updated
        // we need to retry fetching new data
        if(scan.status == INDEX_SCAN_DELETED)
            continue;

        if(scan.status == INDEX_SCAN_EOF_REACHED) {
            debug("[-] index scan: next-header: eof reached\n");
            fileid += 1;
        }
    }

    // never reached
}

static index_scan_t index_last_header_real(index_scan_t scan) {
    index_item_t source;

    debug("[+] index scan: last-header: trying previous offset: %lu\n", scan.target);

    // at that point, we know scan.target is set to the expected value
    if(scan.target == 0) {
        debug("[+] index scan: last-header: zero reached, nothing to rollback\n");
        return index_scan_error(scan, INDEX_SCAN_NO_MORE_DATA);
    }

    // jumping to previous object
    lseek(scan.fd, scan.target, SEEK_SET);

    // reading the fixed-length
    if(read(scan.fd, &source, sizeof(index_item_t)) != sizeof(index_item_t)) {
        warnp("index scan: previous-header: could not read previous offset indexfile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    index_item_header_dump(&source);

    // checking if entry is deleted
    if(source.flags & INDEX_ENTRY_DELETED) {
        debug("[+] index scan: last-header: data is deleted, going one further\n");

        // reset target, so next time we come here, we will refetch previous
        // entry and use the mechanism to check if it's the previous file and
        // so on
        scan.target = source.previous;

        // let's notify source this entry was deleted and we
        // should retrigger the fetch
        return index_scan_error(scan, INDEX_SCAN_DELETED);
    }

    if(!(scan.header = (index_item_t *) malloc(sizeof(index_item_t) + source.idlength))) {
        warnp("data: last-header: malloc");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        warnp("data: last-header: could not read id from datafile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    debug("[+] data: last-header: entry found\n");
    scan.status = INDEX_SCAN_SUCCESS;

    return scan;
}


index_scan_t index_last_header(index_root_t *root) {
    uint16_t fileid = root->indexid;
    index_scan_t scan = {
        .fd = 0,
        .original = root->previous, // offset of the last key
        .target = root->previous,   // again offset of the last key
        .header = NULL,
        .status = INDEX_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire data id fd
        if((scan.fd = index_grab_fileid(root, fileid)) < 0) {
            debug("[-] index scan: last-header: could not open requested file id (%u)\n", fileid);
            return index_scan_error(scan, INDEX_SCAN_NO_MORE_DATA);
        }

        // trying to get entry
        scan = index_last_header_real(scan);

        // release fileid
        index_release_fileid(root, fileid, scan.fd);

        if(scan.status == INDEX_SCAN_SUCCESS)
            return scan;

        if(scan.status == INDEX_SCAN_UNEXPECTED)
            return scan;

        if(scan.status == INDEX_SCAN_NO_MORE_DATA)
            return scan;

        // entry was deleted, scan object is updated
        // we need to retry fetching new data
        if(scan.status == INDEX_SCAN_DELETED)
            continue;

        if(scan.status == INDEX_SCAN_REQUEST_PREVIOUS)
            fileid -= 1;
    }

    // never reached
}


