#ifndef __RKV_DATA_H
    #define __RKV_DATA_H

    typedef struct data_t {
        char *datadir;
        char *datafile;
        uint16_t dataid;
        int datafd;

    } data_t;

    typedef struct data_header_t {
        uint64_t id;
        uint32_t length;

    } __attribute__((packed)) data_header_t;

    void data_init(uint16_t dataid);
    void data_destroy();
    size_t data_jump_next();
    void data_emergency();

    char *data_get(size_t offset, size_t length, uint16_t dataid);
    size_t data_insert(char *buffer, uint64_t id, uint32_t length);
#endif
