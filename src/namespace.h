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

    } __attribute__((packed)) data_header_t;

    typedef struct namespace_t {
        unsigned char *name;
        unsigned char *password;
        int public;

    } namespace_t;

    typedef struct namespaces_t {
        size_t length;
        namespace_t **namespaces;

    } namespaces_t;

#endif
