#ifndef __ZDB_DATA_H
    #define __ZDB_DATA_H

    // split datafile after 256 MB
    #define ZDB_DEFAULT_DATA_MAXSIZE  256 * 1024 * 1024
    #define ZDB_DATA_MAX_PAYLOAD      8 * 1024 * 1024

    // data statistics
    typedef struct data_stats_t {
        size_t hits;     // amount of data hit requested (not used yet)
        size_t faults;   // amount of data hit missed (not used yet)
        size_t errors;   // amount of io (read/write) error
        time_t lasterr;  // last error timestamp

    } data_stats_t;


    // root point of the memory handler
    // used by the data manager
    typedef struct data_root_t {
        char *datadir;      // root path of the data files
        char *datafile;     // name of current datafile in use
        fileid_t dataid;    // id of the datafile currently in use
        int datafd;         // file descriptor of the current datafile in use
        int sync;           // flag to force data write sync
        int synctime;       // force to sync data after this timeout (on next write)
        time_t lastsync;    // keep track when the last sync was explictly made
        size_t previous;    // keep latest offset inserted to the datafile
        int secure;         // enable some safety (see secure zdb_settings_t)
        data_stats_t stats; // data statistics (session time)

    } data_root_t;

    // data file header
    // this file is more there for information
    // this is not really relevant but can be used
    // to validate contents and detect type with the magic
    typedef struct data_header_t {
        char magic[4];     // four byte magic to recognize the file
        uint32_t version;  // file version, for eventual upgrade compatibility
        uint64_t created;  // unix timestamp of creation time
        uint64_t opened;   // unix timestamp of last opened time
        fileid_t fileid;   // current index file id (sync with dataid)

    } __attribute__((packed)) data_header_t;

    typedef enum data_flags_t {
        DATA_ENTRY_DELETED   = 1,       // flag entry as deleted
        DATA_ENTRY_TRUNCATED = 1 << 1,  // used on compaction, tell entry was truncated

    } data_flags_t;

    // data_header_t contains header of each entry in the datafile
    // this header doesn't contain the payload, we assume the payload
    // follows the header
    typedef struct data_entry_header_t {
        uint8_t idlength;     // length of the id
        uint32_t datalength;  // length of the payload
        uint32_t previous;    // previous entry offset
        uint32_t integrity;   // simple integrity check (crc32)
        uint8_t flags;        // keep deleted flags (should be data_flags_t type)
        uint32_t timestamp;   // when did the entry was created (unix timestamp)
        char id[];            // accessor to the id, dynamically

    } __attribute__((packed)) data_entry_header_t;

    // struct used to return data entry from a datafile
    // this struct contains length, which can be filled
    // by the data algorythm if we need to extract length
    // from data header (directkey mode for example)
    typedef struct data_payload_t {
        unsigned char *buffer;
        size_t length;

    } data_payload_t;

    typedef enum data_error_t {
        DATA_RAW_EOF = 1,

    } data_error_t;

    typedef struct data_raw_t {
        data_entry_header_t header;
        uint8_t *id;
        data_payload_t payload;
        data_error_t error;

    } data_raw_t;

    // scan internal representation
    // we use a status and a pointer to the header
    // in order to know what to do
    typedef enum data_scan_status_t {
        DATA_SCAN_SUCCESS,          // requested data found
        DATA_SCAN_REQUEST_PREVIOUS, // requested offset found in the previous datafile
        DATA_SCAN_EOF_REACHED,      // end of datafile reached, last key of next datafile requested
        DATA_SCAN_UNEXPECTED,       // unexpected (memory, ...) error
        DATA_SCAN_NO_MORE_DATA,     // last item has been requested, no more data
        DATA_SCAN_DELETED,          // entry was deleted, scan is updated to go to the next entry

    } data_scan_status_t;

    typedef struct data_scan_t {
        int fd;           // data file descriptor
        size_t original;  // offset of the original key requested
        size_t target;    // offset of the target key (read from the original)
                          // target will be 0 on the first call
                          // target will be updated if the offset is in another datafile

        data_entry_header_t *header;  // target header, set when found
        data_scan_status_t status;    // status code

    } data_scan_t;

    // struct to pass to data operation
    // in order to reduce arguments length
    typedef struct data_request_t {
        unsigned char *data;
        uint32_t datalength;
        void *vid;
        uint8_t idlength;
        uint8_t flags;
        uint32_t crc;
        time_t timestamp;

    } data_request_t;

    data_root_t *data_init(zdb_settings_t *settings, char *datapath, fileid_t dataid);
    data_root_t *data_init_lazy(zdb_settings_t *settings, char *datapath, fileid_t dataid);
    int data_open_id_mode(data_root_t *root, fileid_t id, int mode);

    data_header_t *data_descriptor_load(data_root_t *root);
    data_header_t *data_descriptor_validate(data_header_t *header, data_root_t *root);

    void data_destroy(data_root_t *root);
    size_t data_jump_next(data_root_t *root, fileid_t newid);
    void data_emergency(data_root_t *root);
    fileid_t data_dataid(data_root_t *root);
    void data_delete_files(char *datadir);

    data_raw_t data_raw_get(data_root_t *root, fileid_t dataid, off_t offset);
    data_payload_t data_get(data_root_t *root, size_t offset, size_t length, fileid_t dataid, uint8_t idlength);
    int data_check(data_root_t *root, size_t offset, fileid_t dataid);

    // size_t data_match(data_root_t *root, void *id, uint8_t idlength, size_t offset, fileid_t dataid);

    int data_delete(data_root_t *root, void *id, uint8_t idlength, time_t timestamp);

    // size_t data_insert(data_root_t *root, unsigned char *data, uint32_t datalength, void *vid, uint8_t idlength, uint8_t flags);
    size_t data_insert(data_root_t *root, data_request_t *source);
    size_t data_next_offset(data_root_t *root);

    data_scan_t data_previous_header(data_root_t *root, fileid_t dataid, size_t offset);
    data_scan_t data_next_header(data_root_t *root, fileid_t dataid, size_t offset);
    data_scan_t data_first_header(data_root_t *root);
    data_scan_t data_last_header(data_root_t *root);

    int data_entry_is_deleted(data_entry_header_t *entry);
#endif
