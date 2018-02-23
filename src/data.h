#ifndef __ZDB_DATA_H
    #define __ZDB_DATA_H

    // root point of the memory handler
    // used by the data manager
    typedef struct data_root_t {
        char *datadir;    // root path of the data files
        char *datafile;   // pointer to the current datafile used
        uint16_t dataid;  // id of the datafile currently in use
        int datafd;       // file descriptor of the current datafile used
        int sync;         // flag to force data write sync
        int synctime;     // force to sync data after this timeout (on next write)
        time_t lastsync;  // keep track when the last sync was explictly made

    } data_root_t;

    // data file header
    // this file is more there for information
    // this is not really relevant but can be used
    // to validate contents and detect type with the magic
    typedef struct data_header_t {
        char magic[4];     // four bytes magic bytes to recognize the file
        uint32_t version;  // file version, for possible upgrade compatibility
        uint64_t created;  // unix timestamp of creation time
        uint64_t opened;   // unix timestamp of last opened time
        uint16_t fileid;   // current index file id (sync with dataid)

    } __attribute__((packed)) data_header_t;

    // data_header_t contains header of each entry on the datafile
    // this header doesn't contains the payload, we assume the payload
    // follows the header
    typedef struct data_entry_header_t {
        uint8_t idlength;     // length of the id
        uint32_t datalength;  // length of the payload
        uint32_t integrity;   // simple integrity check (crc32)
        char id[];            // accessor to the id, dynamically

    } __attribute__((packed)) data_entry_header_t;

    // struct used to return data from datafile
    // this struct contains length, which can be filled
    // by the data algorythm if we need to extract length
    // from data header (directkey mode for exemple)
    typedef struct data_payload_t {
        unsigned char *buffer;
        size_t length;

    } data_payload_t;

    data_root_t *data_init(settings_t *settings, char *datapath, uint16_t dataid);
    void data_destroy(data_root_t *root);
    size_t data_jump_next(data_root_t *root, uint16_t newid);
    void data_emergency(data_root_t *root);
    uint16_t data_dataid(data_root_t *root);

    data_payload_t data_get(data_root_t *root, size_t offset, size_t length, uint16_t dataid, uint8_t idlength);
    int data_check(data_root_t *root, size_t offset, uint16_t dataid);

    size_t data_insert(data_root_t *root, unsigned char *data, uint32_t datalength, void *vid, uint8_t idlength);
    size_t data_next_offset(data_root_t *root);
#endif
