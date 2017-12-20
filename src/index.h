#ifndef __RKV_INDEX_H
    #define __RKV_INDEX_H

    #define HASHSIZE 32  // sha-256

    typedef struct index_t {
        char magic[4];
        uint32_t version;
        uint32_t created;
        uint32_t opened;

    } index_t;

    typedef struct index_entry_t {
        unsigned char hash[HASHSIZE];
        uint64_t offset;
        uint64_t length;
        uint8_t flags;

    } __attribute__((packed)) index_entry_t;

    typedef struct index_branch_t {
        size_t length;
        size_t next;
        index_entry_t **entries;

    } index_branch_t;

    typedef struct index_root_t {
        char *indexfile;
        int indexfd;
        index_branch_t *branches[256];

    } index_root_t;

    void index_init();
    void index_destroy();

    index_entry_t *index_entry_get(unsigned char *hash);
    index_entry_t *index_entry_insert(unsigned char *hash, size_t offset, size_t length);
    index_entry_t *index_entry_insert_memory(unsigned char *hash, size_t offset, size_t length);
#endif
