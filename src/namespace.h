#ifndef __ZDB_NAMESPACE_H
    #define __ZDB_NAMESPACE_H

    // default namespace name for new clients
    // this namespace will be used for any unauthentificated clients
    // or any action without namespace specified
    #define NAMESPACE_DEFAULT  "default"

    typedef enum ns_flags_t {
        NS_FLAGS_PUBLIC = 1,  // public read-only namespace

    } ns_flags_t;

    // ns_header_t contains header about a specific namespace
    // this header will be the only contents of the namespace descriptor file
    typedef struct ns_header_t {
        uint8_t namelength;       // length of the namespace name
        uint8_t passlength;       // length of the password
        uint32_t maxsize;         // maximum datasize allowed on that namespace
        uint8_t flags;            // some flags (see define below)
        unsigned char *name;      // accessor to the name
        unsigned char *password;  // accessor to the password

    } __attribute__((packed)) ns_header_t;

    typedef struct namespace_t {
        char *name;
        char *password;
        char *indexpath;
        char *datapath;
        index_root_t *index;
        data_root_t *data;
        int public;

    } namespace_t;

    typedef struct ns_root_t {
        size_t length;             // amount of namespaces allocated
        namespace_t **namespaces;  // pointers to namespaces
        settings_t *settings;      // global settings reminder
        index_branch_t **branches; // unique global branches list

        // as explained on namespace.c, we keep a single big one
        // index which contains everything (all namespaces together)
        //
        // for each index structure, we will point the branches to the
        // same big index branches all the time, this is why we keep
        // this one here, here is the 'original one'

    } ns_root_t;

    int namespace_init(settings_t *settings);
    int namespace_destroy();
    int namespace_emergency();

    namespace_t *namespace_get_default();
#endif
