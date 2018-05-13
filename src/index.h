#ifndef __ZDB_INDEX_H
    #define __ZDB_INDEX_H

    // index file header
    // this file is more there for information
    // this is not really relevant but can be used
    // to validate contents and detect type with the magic
    typedef struct index_t {
        char magic[4];     // four bytes magic bytes to recognize the file
        uint32_t version;  // file version, for possible upgrade compatibility
        uint64_t created;  // unix timestamp of creation time
        uint64_t opened;   // unix timestamp of last opened time
        uint16_t fileid;   // current index file id (sync with dataid)
        uint8_t mode;      // running mode when index was created

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

    typedef enum index_flags_t {
        INDEX_ENTRY_DELETED = 1,  // we keep entry in memory and flag it as deleted

    } index_flags_t;

    typedef struct index_entry_t {
        // linked list pointer
        struct index_entry_t *next;

        // pointer to source namespace
        // index should not be aware of his namespace
        // but since we use a single big index, we need to
        // be able to make namespace distinction
        // note: another approch could be separate branch-list per namespace
        // note 2: we keep a void pointer, we will only compare address and not
        //         the object itself, this make some opacity later if we change
        //         and reduce issue with circular inclusion
        void *namespace;

        uint8_t idlength;    // length of the id, here uint8_t limits to 256 bytes
        uint64_t offset;     // offset on the corresponding datafile
        uint64_t length;     // length of the payload on the datafile
        uint8_t flags;       // keep deleted flags (should be index_flags_t type)
        uint16_t dataid;     // datafile id where is stored the payload
        unsigned char id[];  // the id accessor, dynamically loaded

    } index_entry_t;

    // WARNING: this should be on index_branch.h
    //          but we can't due to cirtucal dependencies
    //          in order to fix this, we should put all struct in a dedicated file
    //
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

    // index status flags
    // keep some heatly status of the index
    typedef enum index_status_t {
        INDEX_NOT_LOADED = 1,       // index not initialized yet
        INDEX_HEALTHY    = 1 << 1,  // no issue detected
        INDEX_READ_ONLY  = 1 << 2,  // index filesystem is read-only
        INDEX_DEGRADED   = 1 << 3,  // some error occured during index loads

    } index_status_t;

    //
    // global root memory structure of the index
    //
    typedef struct index_root_t {
        char *indexdir;     // directory where index files are
        char *indexfile;    // current index filename in use
        uint16_t indexid;   // current index file id in use (sync with data id)
        int indexfd;        // current file descriptor used
        uint32_t nextentry; // next-entry is a global id used in sequential mode (next seq-id)
        int sync;           // flag to force write sync
        int synctime;       // force sync index after this amount of time
        time_t lastsync;    // keep track when the last sync was explictly made
        db_mode_t mode;     // running mode for that index

        void *namespace;    // see index_entry_t, same reason

        index_branch_t **branches; // list of branches explained later
        index_status_t status;     // index health

        size_t datasize;    // statistics about size of data on disk
        size_t indexsize;   // statistics about index in-memory size
        size_t entries;     // statistics about number of keys for this index

    } index_root_t;

    // key used by direct mode
    typedef struct index_dkey_t {
        uint16_t dataid;
        uint32_t offset;

    } __attribute__((packed)) index_dkey_t;

    // key length is uint8_t
    #define MAX_KEY_LENGTH  (1 << 8) - 1

    size_t index_jump_next(index_root_t *root);
    int index_emergency(index_root_t *root);
    uint64_t index_next_id(index_root_t *root);

    index_entry_t *index_entry_get(index_root_t *root, unsigned char *id, uint8_t length);
    index_entry_t *index_entry_insert(index_root_t *root, void *vid, uint8_t idlength, size_t offset, size_t length);
    index_entry_t *index_entry_insert_memory(index_root_t *root, unsigned char *id, uint8_t idlength, size_t offset, size_t length, uint8_t flags);

    index_entry_t *index_entry_delete(index_root_t *root, index_entry_t *entry);
    int index_entry_is_deleted(index_entry_t *entry);

    int index_clean_namespace(index_root_t *root, void *namespace);

    extern index_entry_t *index_reusable_entry;

    // extern but not really public functions
    // used by index_loader
    int index_write(int fd, void *buffer, size_t length, index_root_t *root);
    void index_set_id(index_root_t *root);
    void index_open_final(index_root_t *root);

    extern index_item_t *index_transition;
    extern index_entry_t *index_reusable_entry;

    size_t index_next_offset(index_root_t *root);
    uint16_t index_indexid(index_root_t *root);
#endif
