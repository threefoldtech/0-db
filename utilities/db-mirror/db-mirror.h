#ifndef ZDB_DB_MIRROR
    #define ZDB_DB_MIRROR

    #define MB(x)   (x / (1024 * 1024.0))

    //
    // main database context
    //
    typedef struct sync_t {
        redisContext *source;  // source payload

        unsigned int remotes;
        redisContext **targets;

    } sync_t;

    //
    // payload stats
    //
    typedef struct status_t {
        size_t size;        // total in bytes, to transfert
        size_t transfered;  // total in bytes transfered

        size_t keys;        // amount of keys to transfert
        size_t copied;      // amount of keys transfered
        size_t requested;   // amount of keys requested

    } status_t;

    //
    // namespace context
    //
    typedef struct namespace_t {
        char *name;
        int public;
        char *password;
        int limit;
        int mode;
        status_t status;

    } namespace_t;

    typedef struct namespaces_t {
        namespace_t *list;
        unsigned int length;

    } namespaces_t;


    //
    // interface
    //
    typedef struct keylist_t {
        size_t length;
        size_t allocated;
        size_t size;
        redisReply **keys;

    } keylist_t;

    int replicate(sync_t *sync);
    void diep(char *str);

#endif
