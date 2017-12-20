#ifndef __RKV_INDEX_H
    #define __RKV_INDEX_H


    typedef struct index_t {
        char magic[4];
        uint32_t version;
        uint64_t created;
        uint64_t opened;
        uint16_t fileid;

    } index_t;

    typedef struct index_entry_t {
        unsigned char hash[HASHSIZE];
        uint64_t offset;
        uint64_t length;
        uint8_t flags;
        uint16_t dataid;

    } __attribute__((packed)) index_entry_t;

    typedef struct index_branch_t {
        size_t length;
        size_t next;
        index_entry_t **entries;

    } index_branch_t;

    typedef struct index_root_t {
        char *indexdir;
        char *indexfile;
        uint16_t indexid;
        int indexfd;
        index_branch_t *branches[256];

    } index_root_t;

    uint16_t index_init();
    void index_destroy();
    size_t index_jump_next();
    void index_emergency();

    index_entry_t *index_entry_get(unsigned char *hash);
    index_entry_t *index_entry_insert(unsigned char *hash, size_t offset, size_t length);
    index_entry_t *index_entry_insert_memory(unsigned char *hash, size_t offset, size_t length);
#endif
