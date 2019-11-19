#ifndef __ZDB_INDEX_SCAN_H
    #define __ZDB_INDEX_SCAN_H

    // scan internal representation
    // we use a status and a pointer to the header
    // in order to know what to do
    typedef enum index_scan_status_t {
        INDEX_SCAN_SUCCESS,          // requested index entry found
        INDEX_SCAN_REQUEST_PREVIOUS, // offset requested found in the previous file
        INDEX_SCAN_EOF_REACHED,      // end of file reached, last key of next file requested
        INDEX_SCAN_UNEXPECTED,       // unexpected (memory, ...) error
        INDEX_SCAN_NO_MORE_DATA,     // last item requested, nothing more
        INDEX_SCAN_DELETED,          // entry was deleted, scan is updated to go further

    } index_scan_status_t;

    typedef struct index_scan_t {
        int fd;           // file descriptor
        size_t original;  // offset of the original key requested
        size_t target;    // offset of the target key (read from the original)
                          // target will be 0 on the first call
                          // target will be updated if the offset is in another file
        index_item_t *header;        // target header, set when found
        index_scan_status_t status;  // status code
        uint16_t fileid;             // index file id

    } index_scan_t;

    index_scan_t index_previous_header(index_root_t *root, uint16_t fileid, size_t offset);
    index_scan_t index_next_header(index_root_t *root, uint16_t fileid, size_t offset);
    index_scan_t index_first_header(index_root_t *root);
    index_scan_t index_last_header(index_root_t *root);
#endif
