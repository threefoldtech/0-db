#ifndef __RKV_DATA_H
    #define __RKV_DATA_H

    // root point of the memory handler
    // used by the data manager
    typedef struct data_t {
        char *datadir;    // root path of the data files
        char *datafile;   // pointer to the current datafile used
        uint16_t dataid;  // id of the datafile currently in use
        int datafd;       // file descriptor of the current datafile used
        int sync;         // flag to force data write sync
        int synctime;     // force to sync data after this timeout (on next write)
        time_t lastsync;  // keep track when the last sync was explictly made

    } data_t;

    // data_header_t contains header of each entry on the datafile
    // this header doesn't contains the payload, we assume the payload
    // follows the header
    typedef struct data_header_t {
        uint8_t idlength;     // length of the id
        uint32_t datalength;  // length of the payload
        uint32_t integrity;   // simple integrity check (crc32)
        char id[];            // accessor to the id, dynamically

    } __attribute__((packed)) data_header_t;

    void data_init(uint16_t dataid, settings_t *settings);
    void data_destroy();
    size_t data_jump_next();
    void data_emergency();

    unsigned char *data_get(size_t offset, size_t length, uint16_t dataid, uint8_t idlength);
    size_t data_insert(unsigned char *data, uint32_t datalength, void *vid, uint8_t idlength);
#endif
