#ifndef __RKV_DATA_H
    #define __RKV_DATA_H

    typedef struct data_t {
        char *datadir;
        char *datafile;
        uint16_t dataid;
        int datafd;

    } data_t;

    typedef struct data_header_t {
        unsigned char hash[HASHSIZE];
        uint32_t length;

    } __attribute__((packed)) data_header_t;

    char *sha256_hex(unsigned char *hash);
    unsigned char *sha256_compute(unsigned char *target, const char *buffer, size_t length);
    unsigned char *sha256_parse(char *buffer, unsigned char *target);

    void data_init(uint16_t dataid);
    void data_destroy();
    size_t data_jump_next();
    
    char *data_get(size_t offset, size_t length, uint16_t dataid);
    size_t data_insert(char *buffer, unsigned char *hash, uint32_t length);
#endif
