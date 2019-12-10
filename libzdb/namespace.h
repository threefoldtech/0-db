#ifndef __ZDB_NAMESPACE_H
    #define __ZDB_NAMESPACE_H

    // default namespace name for new clients
    // this namespace will be used for any unauthentificated clients
    // or any action without namespace specified
    #define NAMESPACE_DEFAULT  "default"

    // current expected version of header
    #define NAMESPACE_CURRENT_VERSION  1

    typedef enum ns_flags_t {
        NS_FLAGS_PUBLIC = 1,   // public read-only namespace
        NS_FLAGS_WORM = 2,     // worm mode enabled or not
        NS_FLAGS_EXTENDED = 4, // extended header is present

    } ns_flags_t;

    // ns_header_legacy_t contains header about a specific namespace for previous 0-db
    // version, this header will be the only contents of the namespace descriptor file
    typedef struct ns_header_legacy_t {
        uint8_t namelength;    // length of the namespace name
        uint8_t passlength;    // length of the password
        uint32_t maxsize;      // old maximum size (ignored, see extended)
        uint8_t flags;         // some flags (see define above)

        // this header won't never change, this is the first version
        // deployed, to keep retro-compatibility with old database
        // we keep the same prefix
        //
        // new version contains more fields and contains a version
        // identifier used to keep track of update
        //
        // the flag NS_FLAGS_EXTENDED is set when the header used is
        // any new version, which means you can still load old version
        // and new version will still works on previous version

    } __attribute__((packed)) ns_header_legacy_t;

    typedef struct ns_header_extended_t {
        uint32_t version;  // keep track of this version
        uint64_t maxsize;  // real maxsize encoded on 64 bits (no 4 GB limit)

    } __attribute__((packed)) ns_header_extended_t;


    typedef struct namespace_t {
        char *name;            // namespace string-name
        char *password;        // optional password
        char *indexpath;       // index root directory
        char *datapath;        // data root directory
        index_root_t *index;   // index structure-pointer
        data_root_t *data;     // data structure-pointer
        char public;           // publicly readable (read without password)
        size_t maxsize;        // maximum size allowed
        size_t idlist;         // nsroot list index
        size_t version;        // internal version used
        char worm;             // worm mode (write only read multiple)
                               // this mode disable overwrite/deletion

    } namespace_t;

    typedef struct ns_root_t {
        size_t length;             // amount of namespaces allocated
        size_t effective;          // amount of namespace currently loaded
        namespace_t **namespaces;  // pointers to namespaces
        zdb_settings_t *settings;  // global settings reminder
        index_branch_t **branches; // unique global branches list

        // as explained on namespace.c, we keep a single big one
        // index which contains everything (all namespaces together)
        //
        // for each index structure, we will point the branches to the
        // same big index branches all the time, this is why we keep
        // this one here, here is the 'original one'

    } ns_root_t;

    size_t namespace_length();
    int namespace_valid_name(char *name);

    namespace_t *namespace_iter();
    namespace_t *namespace_iter_next(namespace_t *namespace);

    int namespace_create(char *name);
    int namespace_delete(namespace_t *namespace);
    int namespace_commit(namespace_t *namespace);
    int namespace_flush(namespace_t *namespace);
    int namespace_reload(namespace_t *namespace);
    void namespace_free(namespace_t *namespace);
    namespace_t *namespace_get(char *name);

    int namespaces_init(zdb_settings_t *settings);
    ns_root_t *namespaces_allocate(zdb_settings_t *settings);
    int namespaces_destroy();
    int namespaces_emergency();

    namespace_t *namespace_load(ns_root_t *nsroot, char *name);
    namespace_t *namespace_load_light(ns_root_t *nsroot, char *name, int ensure);

    namespace_t *namespace_get_default();
#endif
