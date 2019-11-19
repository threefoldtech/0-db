#ifndef __ZDB_INDEX_H
    #define __ZDB_INDEX_H

    typedef enum index_mode_t {
        // default key-value store
        ZDB_MODE_KEY_VALUE = 0,

        // auto-generated sequential id
        ZDB_MODE_SEQUENTIAL = 1,

        // id is hard-fixed data position
        ZDB_MODE_DIRECT_KEY = 2,

        // fixed-block length
        ZDB_MODE_DIRECTBLOCK = 3,

        // amount of modes available
        ZDB_MODES

    } index_mode_t;

    // when adding or removing some modes
    // don't forget to adapt correctly the handlers
    // function pointers (basicly for GET and SET)


    // index file header
    // this file is more there for information
    // this is not really relevant but can be used
    // to validate contents and detect type with the magic
    typedef struct index_header_t {
        char magic[4];     // four bytes magic bytes to recognize the file
        uint32_t version;  // file version, for possible upgrade compatibility
        uint64_t created;  // unix timestamp of creation time
        uint64_t opened;   // unix timestamp of last opened time
        uint16_t fileid;   // current index file id (sync with dataid)
        uint8_t mode;      // running mode when index was created

    } __attribute__((packed)) index_header_t;

    // main entry structure
    // each key will use one of this entry
    typedef struct index_item_t {
        uint8_t idlength;    // length of the id, here uint8_t limits to 256 bytes
        uint32_t offset;     // offset on the corresponding datafile
        uint32_t length;     // length of the payload on the datafile
        uint32_t previous;   // previous entry offset

        // WARNING: 'previous' flag was buggy in early version, only in direct mode
        //          the flag was updated on overwrite when it should not have changed
        //          (previous entry in the file doesn't change even if new entries comes)

        uint8_t flags;       // key flags (eg: deleted)
        uint16_t dataid;     // datafile id where is stored the payload
        uint32_t timestamp;  // when was the key created (unix timestamp)
        uint32_t crc;        // the data payload crc32
        uint16_t parentid;   // history parent dataid ]
        uint32_t parentoff;  // history parent offset ]- used to keep history tracking
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
        uint32_t offset;     // offset on the corresponding datafile
        uint32_t idxoffset;  // offset on the index file (index file id is the same as data file)
        uint32_t length;     // length of the payload on the datafile
        uint8_t flags;       // keep deleted flags (should be index_flags_t type)
        uint16_t dataid;     // datafile id where payload is located
        uint16_t indexid;    // indexfile id where this index entry is located
        uint32_t crc;        // the data payload crc32
        uint16_t parentid;   // parent index file id (history)
        uint32_t parentoff;  // parent index file offset (history)
        uint32_t timestamp;  // unix timestamp of key creation
        unsigned char id[];  // the id accessor, dynamically loaded

    } index_entry_t;

    // WARNING: this should be on index_branch.h
    //          but we can't due to circular dependencies
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

    // index sequential id mapping
    typedef struct index_seqmap_t {
        uint32_t seqid;
        uint16_t fileid;

    } index_seqmap_t;

    typedef struct index_seqid_t {
        uint16_t allocated;
        uint16_t length;
        index_seqmap_t *seqmap;

    } index_seqid_t;

    //
    // global root memory structure of the index
    //
    typedef struct index_root_t {
        char *indexdir;     // directory where index files are
        char *indexfile;    // current index filename in use
        uint16_t indexid;   // current index file id in use (sync with data id)
        int indexfd;        // current file descriptor used
        uint64_t nextentry; // next-entry is a global id used in sequential mode (next seq-id)
        uint32_t nextid;    // next-id is a localfile id used in direct mode (next id on this file)
        int sync;           // flag to force write sync
        int synctime;       // force sync index after this amount of time
        time_t lastsync;    // keep track when the last sync was explictly made
        index_mode_t mode;  // running mode for that index

        void *namespace;    // see index_entry_t, same reason

        index_seqid_t *seqid;      // sequential fileid mapping
        index_branch_t **branches; // list of branches explained later
        index_status_t status;     // index health

        size_t datasize;    // statistics about size of data on disk
        size_t indexsize;   // statistics about index in-memory size
        size_t entries;     // statistics about number of keys for this index

        size_t previous;    // keep latest offset inserted to the indexfile

    } index_root_t;

    // key used by direct mode
    // contains information about fileid and
    // objectid, since all object are made of
    // the same length, we can compute offset like this
    typedef struct index_dkey_t {
        uint16_t indexid;
        uint32_t objectid;

    } __attribute__((packed)) index_dkey_t;

    // binary key
    // this is a representation of an index key
    // using binary fields pointing directly to
    // it's file, but with additional information
    // not directly needed to find the value back
    // but useful to ensure the user won't send fake
    // malformed or crafted illegal key
    //
    // eg: requesting fileid 10 and offset 57 is easy...
    //     but requesting fileid 10 and offset 57 with
    //     keylength and crc matching the index entry,
    //     the probability of fake crafted user key is
    //     quite impossible
    typedef struct index_bkey_t {
        uint8_t idlength;
        uint16_t fileid;
        uint32_t length;
        uint32_t idxoffset;
        uint32_t crc;

    } __attribute__((packed)) index_bkey_t;

    // key used to represent exact position
    // in index file (aka: dkey resolved)
    typedef struct index_ekey_t {
        uint16_t indexid;
        uint32_t offset;

    } __attribute__((packed)) index_ekey_t;


    // key length is uint8_t
    #define MAX_KEY_LENGTH  (1 << 8) - 1

    size_t index_jump_next(index_root_t *root);
    int index_emergency(index_root_t *root);

    uint64_t index_next_id(index_root_t *root);
    uint32_t index_next_objectid(index_root_t *root);

    index_entry_t *index_entry_get(index_root_t *root, unsigned char *id, uint8_t length);
    index_item_t *index_item_get_disk(index_root_t *root, uint16_t indexid, size_t offset, uint8_t idlength);

    index_dkey_t *index_dkey_from_key(index_dkey_t *dkey, unsigned char *buffer, uint8_t length);

    index_entry_t *index_entry_insert_new(index_root_t *root, void *vid, index_entry_t *new, time_t timestamp, index_entry_t *existing);
    index_entry_t *index_entry_insert_memory(index_root_t *root, unsigned char *realid, index_entry_t *new);

    int index_entry_delete(index_root_t *root, index_entry_t *entry);
    int index_entry_delete_disk(index_root_t *root, index_entry_t *entry);
    int index_entry_delete_memory(index_root_t *root, index_entry_t *entry);
    int index_entry_is_deleted(index_entry_t *entry);

    int index_clean_namespace(index_root_t *root, void *namespace);

    extern index_entry_t *index_reusable_entry;

    // extern but not really public functions
    // used by index_loader
    int index_write(int fd, void *buffer, size_t length, index_root_t *root);
    void index_set_id(index_root_t *root, uint16_t fileid);
    void index_open_final(index_root_t *root);

    extern index_item_t *index_transition;
    extern index_entry_t *index_reusable_entry;

    size_t index_next_offset(index_root_t *root);
    size_t index_offset_objectid(uint32_t idobj);
    uint16_t index_indexid(index_root_t *root);

    index_bkey_t index_item_serialize(index_item_t *item, uint32_t idxoffset, uint16_t idxfileid);
    index_bkey_t index_entry_serialize(index_entry_t *entry);
    index_entry_t *index_entry_deserialize(index_root_t *root, index_bkey_t *key);

    int index_grab_fileid(index_root_t *root, uint16_t fileid);
    void index_release_fileid(index_root_t *root, uint16_t fileid, int fd);

    void index_item_header_dump(index_item_t *item);
    void index_entry_dump(index_entry_t *entry);

    uint32_t index_key_hash(unsigned char *id, uint8_t idlength);

    // open index _without_ setting internal fd
    int index_open_file_readonly(index_root_t *root, uint16_t fileid);
    int index_open_file_readwrite(index_root_t *root, uint16_t fileid);

    // open index by setting index fd
    int index_open_readonly(index_root_t *root, uint16_t fileid);
    int index_open_readwrite(index_root_t *root, uint16_t fileid);
    int index_open_readwrite_oneshot(index_root_t *root, uint16_t fileid);

    void index_close(index_root_t *root);

    const char *index_modename(index_root_t *index);
#endif
