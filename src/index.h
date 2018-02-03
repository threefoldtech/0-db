#ifndef __RKV_INDEX_H
    #define __RKV_INDEX_H

    // index file header
    // this file is more there for information
    // this is not really relevant but can be used
    // to validate contents and detect type with the magic
    typedef struct index_t {
        char magic[4];     // four bytes magic bytes to recognize the file
        uint32_t version;
        uint64_t created;  // unix timestamp of creation time
        uint64_t opened;   // unix timestamp of last opened time
        uint16_t fileid;   // current index file id (sync with dataid)

    } __attribute__((packed)) index_t;

    // main entry structure
    // each key will use one of this entry
    typedef struct index_item_t {
        uint8_t idlength;    // length of the id, here uint8_t limits to 256 bytes
        uint64_t offset;     // offset on the corresponding datafile
        uint64_t length;     // length of the payload on the datafile
        uint8_t flags;       // flags not used yet, could provide information about deletion
        uint16_t dataid;     // datafile id where is stored the payload
        unsigned char id[];  // the id accessor, dynamically loaded

    } __attribute__((packed)) index_item_t;

    typedef struct index_entry_t {
        // linked list pointer
        struct index_entry_t *next;

        uint8_t idlength;    // length of the id, here uint8_t limits to 256 bytes
        uint64_t offset;     // offset on the corresponding datafile
        uint64_t length;     // length of the payload on the datafile
        uint8_t flags;       // flags not used yet, could provide information about deletion
        uint16_t dataid;     // datafile id where is stored the payload
        unsigned char id[];  // the id accessor, dynamically loaded

    } index_entry_t;

    // the current implementation of the index use rudimental index memory system
    // it's basicly just linked-list of entries
    // to improve performance without changing this basic implementation,
    // which is really slow, of course, we use a "branch" system which simply
    // split all the arrays based on an id
    //
    // the id is specified on the implementation file, with the length, etc.
    //
    // - id 0000: [...........]
    // - id 0001: [...................]
    // - id 0002: [...]
    typedef struct index_branch_t {
        size_t length;       // length of this branch (count of entries)
        index_entry_t *list; // entry point of the linked list
        index_entry_t *last; // pointer to the last item, quicker to append

    } index_branch_t;

    // global root memory structure of the index
    typedef struct index_root_t {
        char *indexdir;     // directory where index files are
        char *indexfile;    // current index filename in use
        uint16_t indexid;   // current index file id in use (sync with data id)
        int indexfd;        // current file descriptor used
        uint64_t nextentry; // next-entry is a global id used in sequential mode (next seq-id)
        index_branch_t **branches; // list of branches explained later

    } index_root_t;

    // flags values
    #define INDEX_ENTRY_DELETED      1  // we keep deleted flags not keep entry in memory

    // key length is uint8_t
    #define MAX_KEY_LENGTH  (1 << 8) - 1

    uint16_t index_init(char *indexpath, int dump);
    void index_destroy();
    size_t index_jump_next();
    void index_emergency();
    uint64_t index_next_id();

    index_entry_t *index_entry_get(unsigned char *id, uint8_t length);
    index_entry_t *index_entry_insert(unsigned char *id, uint8_t idlength, size_t offset, size_t length);
    index_entry_t *index_entry_insert_memory(unsigned char *id, uint8_t idlength, size_t offset, size_t length, uint8_t flags);
    index_entry_t *index_entry_delete(unsigned char *id, uint8_t length);
#endif
