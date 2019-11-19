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
#include "libzdb.h"
#include "libzdb_private.h"

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

//
// oops, some mistake happened
//
// if you read this, code version is fixed and this is a hotfix
// on runtime to support 'buggy' index files
//
// before this fix, in sequential mode, when data were overwritten
// a new entry is was added and flagged as deleted and original entry
// was updated to point to new data location, this is the only
// way to be able to read forward the index and know entries are deleted
// or available without reading the full index
//
// problem was, when updating the original key, the 'previous' field was
// overwritten with the new value, which breaks the backward link, 'previous'
// field was pointing to the new previous location which is completely wrong,
// the real previous offset is still the object just before.
//
// since this only occures in sequential mode where key are fixed length,
// we can compute previous offset by just substracting entry size, we do
// that on runtime and ignore the field on the file
//
// if we hit the begining of the index file, we will use a special value '1' as
// offset to know we need to fetch previous index file and start reading from
// the end of the file, because we can't predict the offset of the last entry
// on the other file, we will need to figure out on runtime too
//
// this was bad, sorry
//
void __ditry_seqmode_fix(index_item_t *source, off_t original) {
    // this fix only apply to direct (sequential) mode
    // skipping any other mode
    if(zdb_rootsettings.mode != ZDB_MODE_SEQUENTIAL && zdb_rootsettings.mode != ZDB_MODE_DIRECT_KEY)
        return;

    // compute previous offset
    // since sequential keys are hardcoded to be 4 bytes, we can easily
    // compute previous offset and ignoring value in the file
    //
    // this value were incorrect in some early version
    //
    source->previous = original - sizeof(index_item_t) - sizeof(uint32_t);

    if(source->previous == sizeof(index_header_t))
        source->previous = 1;
}

// RSCAN implementation
static index_scan_t index_previous_header_real(index_scan_t scan) {
    index_item_t source;

    // special dirty-fix case, please see __ditry_seqmode_fix function
    if(scan.target == 1) {
        // forcing last entry position
        scan.target = lseek(scan.fd, 0, SEEK_END);
        scan.target -= sizeof(index_item_t) + sizeof(uint32_t);
    }

    // if scan.target is not set yet, we don't know the expected
    // offset of the previous header, let's read the header
    // of the current entry and find out which is the next one
    if(scan.target == 0) {
        off_t current = lseek(scan.fd, scan.original, SEEK_SET);

        if(read(scan.fd, &source, sizeof(index_item_t)) != sizeof(index_item_t)) {
            zdb_warnp("index rscan: previous-header: could not read original offset file");
            return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
        }

        __ditry_seqmode_fix(&source, scan.original);
        index_item_header_dump(&source);

        if(source.previous >= current) {
            zdb_debug("[+] index rscan: previous-header: previous offset (%u) in previous file\n", source.previous);
            return index_scan_error(scan, INDEX_SCAN_REQUEST_PREVIOUS);
        }

        scan.target = source.previous;

        // rollback special case when pointing to the first entry
        if(scan.target == 1)
            scan.target = sizeof(index_header_t);
    }

    zdb_debug("[+] index rscan: previous-header: offset: %u\n", source.previous);

    // at that point, we know scan.target is set to the expected value
    if(scan.target == 0) {
        zdb_debug("[+] index rscan: previous-header: zero reached, nothing to rollback\n");
        return index_scan_error(scan, INDEX_SCAN_NO_MORE_DATA);
    }

    // special dirty-fix case, please see __ditry_seqmode_fix function
    if(scan.target == 1) {
        scan.target = 0;
        return index_scan_error(scan, INDEX_SCAN_REQUEST_PREVIOUS);
    }

    // jumping to previous object
    lseek(scan.fd, scan.target, SEEK_SET);

    // reading the fixed-length
    if(read(scan.fd, &source, sizeof(index_item_t)) != sizeof(index_item_t)) {
        zdb_warnp("index rscan: previous-header: could not read previous offset datafile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    __ditry_seqmode_fix(&source, scan.target);

    // checking if entry is deleted
    if(source.flags & INDEX_ENTRY_DELETED) {
        zdb_debug("[+] index rscan: previous-header: data is deleted, going one more before\n");

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
        zdb_warnp("index rscan: previous-header: malloc");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        zdb_warnp("index rscan: previous-header: could not read id from datafile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    zdb_debug("[+] index rscan: previous-header: entry found\n");
    scan.status = INDEX_SCAN_SUCCESS;

    return scan;
}

index_scan_t index_previous_header(index_root_t *root, uint16_t fileid, size_t offset) {
    index_scan_t scan = {
        .fd = 0,
        .fileid = fileid,
        .original = offset, // offset of the 'current' key
        .target = 0,        // offset of the 'previous' key
        .header = NULL,     // the previous header
        .status = INDEX_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire file id fd
        if((scan.fd = index_grab_fileid(root, scan.fileid)) < 0) {
            zdb_debug("[-] index rscan: previous-header: could not open requested file id (%u)\n", scan.fileid);
            return index_scan_error(scan, INDEX_SCAN_NO_MORE_DATA);
        }

        // trying to get entry
        scan = index_previous_header_real(scan);

        // release fileid
        index_release_fileid(root, scan.fileid, scan.fd);

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
            scan.fileid = scan.fileid - 1;
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
            zdb_warnp("index scan: next-header: could not read original offset indexfile");
            return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
        }

        zdb_debug("[+] index scan: next-header: this id length: %u\n", source.idlength);

        // next header is at offset + this header + payload
        scan.target = scan.original + sizeof(index_item_t);
        scan.target += source.idlength;
    }

    // jumping to next object
    lseek(scan.fd, scan.target, SEEK_SET);

    // reading the fixed-length
    if(read(scan.fd, &source, sizeof(index_item_t)) != sizeof(index_item_t)) {
        // zdb_warnp("index scan: next-header: could not read next offset indexfile");
        // this mean the entry expected is the first of the next indexfile
        scan.target = sizeof(index_header_t);
        return index_scan_error(scan, INDEX_SCAN_EOF_REACHED);
    }

    // checking if entry is deleted
    if(source.flags & INDEX_ENTRY_DELETED) {
        zdb_debug("[+] index scan: next-header: offset %lu deleted, going one further\n", scan.target);

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
        zdb_warnp("index scan: next-header: malloc");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        zdb_warnp("index scan: next-header: could not read id from datafile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    index_item_header_dump(&source);

    zdb_debug("[+] index scan: next-header: entry found\n");
    scan.status = INDEX_SCAN_SUCCESS;

    return scan;
}

index_scan_t index_next_header(index_root_t *root, uint16_t fileid, size_t offset) {
    index_scan_t scan = {
        .fd = 0,
        .fileid = fileid,
        .original = offset, // offset of the 'current' key
        .target = 0,        // offset of the expected next header
        .header = NULL,     // the new header
        .status = INDEX_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire file id fd
        if((scan.fd = index_grab_fileid(root, fileid)) < 0) {
            zdb_debug("[-] index scan: next-header: could not open requested file id (%u)\n", fileid);
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
            zdb_debug("[-] index scan: next-header: eof reached\n");
            fileid += 1;
            scan.fileid = fileid;
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
        zdb_warnp("data: first-header: could not read next offset datafile");
        // this mean the data expected is the first of the next datafile
        scan.target = sizeof(index_header_t);
        return index_scan_error(scan, INDEX_SCAN_EOF_REACHED);
    }

    index_item_header_dump(&source);

    // checking if entry is deleted
    if(source.flags & INDEX_ENTRY_DELETED) {
        // zdb_debug("[+] data: first-header: data is deleted, going one further\n");

        // jump to the next entry
        scan.target += sizeof(index_item_t);
        scan.target += source.idlength;

        // let's notify source this entry was deleted and we
        // should retrigger the fetch
        return index_scan_error(scan, INDEX_SCAN_DELETED);
    }

    if(!(scan.header = (index_item_t *) malloc(sizeof(index_item_t) + source.idlength))) {
        zdb_warnp("index scan: first-header: malloc");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        zdb_warnp("index scan: first-header: could not read id from datafile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    zdb_debug("[+] index scan: first-header: entry found\n");
    scan.status = INDEX_SCAN_SUCCESS;

    return scan;
}


index_scan_t index_first_header(index_root_t *root) {
    index_scan_t scan = {
        .fd = 0,
        .fileid = 0,
        .original = sizeof(index_header_t), // offset of the first key
        .target = sizeof(index_header_t),   // again offset of the first key
        .header = NULL,
        .status = INDEX_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire data id fd
        if((scan.fd = index_grab_fileid(root, scan.fileid)) < 0) {
            zdb_debug("[-] index scan: first-header: could not open requested file id (%u)\n", scan.fileid);
            return index_scan_error(scan, INDEX_SCAN_NO_MORE_DATA);
        }

        scan = index_first_header_real(scan);

        // release fileid
        index_release_fileid(root, scan.fileid, scan.fd);

        if(scan.status == INDEX_SCAN_SUCCESS)
            return scan;

        if(scan.status == INDEX_SCAN_UNEXPECTED)
            return scan;

        // entry was deleted, scan object is updated
        // we need to retry fetching new data
        if(scan.status == INDEX_SCAN_DELETED)
            continue;

        if(scan.status == INDEX_SCAN_EOF_REACHED) {
            zdb_debug("[-] index scan: next-header: eof reached\n");
            scan.fileid = scan.fileid + 1;
        }
    }

    // never reached
}

static index_scan_t index_last_header_real(index_scan_t scan) {
    index_item_t source;

    zdb_debug("[+] index scan: last-header: trying previous offset: %lu\n", scan.target);

    // at that point, we know scan.target is set to the expected value
    if(scan.target == 0) {
        zdb_debug("[+] index scan: last-header: zero reached, nothing to rollback\n");
        return index_scan_error(scan, INDEX_SCAN_NO_MORE_DATA);
    }

    // special dirty-fix case, please see __ditry_seqmode_fix function
    if(scan.target == 1) {
        // forcing last entry position
        scan.target = lseek(scan.fd, 0, SEEK_END);
        scan.target -= sizeof(index_item_t) + sizeof(uint32_t);
    }

    // jumping to previous object
    lseek(scan.fd, scan.target, SEEK_SET);

    // reading the fixed-length
    if(read(scan.fd, &source, sizeof(index_item_t)) != sizeof(index_item_t)) {
        zdb_warnp("index scan: previous-header: could not read previous offset indexfile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    __ditry_seqmode_fix(&source, scan.target);
    index_item_header_dump(&source);

    // checking if entry is deleted
    if(source.flags & INDEX_ENTRY_DELETED) {
        off_t current = scan.target;

        zdb_debug("[+] index scan: last-header: data is deleted, going one previous\n");

        // reset target, so next time we come here, we will refetch previous
        // entry and use the mechanism to check if it's the previous file and
        // so on
        scan.target = source.previous;

        if(source.previous >= current || source.previous == 1) {
            zdb_debug("[+] index scan: last-header: previous offset in previous file\n");
            return index_scan_error(scan, INDEX_SCAN_REQUEST_PREVIOUS);
        }

        // let's notify source this entry was deleted and we
        // should retrigger the fetch
        return index_scan_error(scan, INDEX_SCAN_DELETED);
    }

    if(!(scan.header = (index_item_t *) malloc(sizeof(index_item_t) + source.idlength))) {
        zdb_warnp("data: last-header: malloc");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    // reading the full header to target
    *scan.header = source;

    if(read(scan.fd, scan.header->id, scan.header->idlength) != (ssize_t) scan.header->idlength) {
        zdb_warnp("data: last-header: could not read id from datafile");
        return index_scan_error(scan, INDEX_SCAN_UNEXPECTED);
    }

    zdb_debug("[+] data: last-header: entry found\n");
    scan.status = INDEX_SCAN_SUCCESS;

    return scan;
}


index_scan_t index_last_header(index_root_t *root) {
    index_scan_t scan = {
        .fd = 0,
        .fileid = root->indexid,
        .original = root->previous, // offset of the last key
        .target = root->previous,   // again offset of the last key
        .header = NULL,
        .status = INDEX_SCAN_UNEXPECTED,
    };

    while(1) {
        // acquire data id fd
        if((scan.fd = index_grab_fileid(root, scan.fileid)) < 0) {
            zdb_debug("[-] index scan: last-header: could not open requested file id (%u)\n", scan.fileid);
            return index_scan_error(scan, INDEX_SCAN_NO_MORE_DATA);
        }

        // trying to get entry
        scan = index_last_header_real(scan);

        // release fileid
        index_release_fileid(root, scan.fileid, scan.fd);

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
            scan.fileid = scan.fileid - 1;
    }

    // never reached
}


