#ifndef __RKV_DATA_H
    #define __RKV_DATA_H

    typedef struct data_t {
        char *datadir;
        char *datafile;
        uint16_t dataid;
        int datafd;

    } data_t;

    typedef struct data_header_t {
        uint8_t idlength;
        uint32_t datalength;
        char id[];

    } __attribute__((packed)) data_header_t;

    void data_init(uint16_t dataid);
    void data_destroy();
    size_t data_jump_next();
    void data_emergency();

    unsigned char *data_get(size_t offset, size_t length, uint16_t dataid, uint8_t idlength);
    size_t data_insert(unsigned char *data, uint32_t datalength, unsigned char *id, uint8_t idlength);
#endif
