#ifndef __RKV_DATA_H
    #define __RKV_DATA_H

    typedef struct data_t {
        char *datafile;
        int datafd;

    } data_t;

    typedef struct data_header_t {
        unsigned char hash[HASHSIZE];
        uint32_t length;

    } __attribute__((packed)) data_header_t;

    char *sha256_hex(unsigned char *hash);
    unsigned char *sha256_compute(unsigned char *target, const char *buffer, size_t length);
    unsigned char *sha256_parse(char *buffer, unsigned char *target);

    void data_init();
    void data_destroy();
    
    char *data_get(size_t offset, size_t length);
    size_t data_insert(char *buffer, unsigned char *hash, uint32_t length);
#endif
