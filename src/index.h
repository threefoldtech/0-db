#ifndef __RKV_INDEX_H
    #define __RKV_INDEX_H

    // index file header
    // this file is more there for information
    // this is not really relevant but can be used
    // to validate contents and detect type with the magic
    typedef struct index_t {
        char magic[4];
        uint32_t version;
        uint64_t created;
        uint64_t opened;
        uint16_t fileid;

    } index_t;

    // this struct is used to read/write on disk
    typedef struct index_entry_t {
        uint8_t idlength;
        uint64_t offset;
        uint64_t length;
        uint8_t flags;
        uint16_t dataid;
        unsigned char id[];

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
        uint64_t nextentry;
        index_branch_t *branches[256];

    } index_root_t;

    typedef struct index_response_t {
        char *response;
        char status;

    } index_response_t;

    // key length is uint8_t
    #define MAX_KEY_LENGTH  (1 << 8) - 1

    uint16_t index_init();
    void index_destroy();
    size_t index_jump_next();
    void index_emergency();
    uint64_t index_next_id();

    index_entry_t *index_entry_get(unsigned char *id, uint8_t length);
    index_entry_t *index_entry_insert(unsigned char *id, uint8_t idlength, size_t offset, size_t length);
    index_entry_t *index_entry_insert_memory(unsigned char *id, uint8_t idlength, size_t offset, size_t length);
#endif
