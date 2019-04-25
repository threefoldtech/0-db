#ifndef __ZDB_API_H
    #define __ZDB_API_H

    typedef enum zdb_api_type_t {
        ZDB_API_SUCCESS,
        ZDB_API_FAILURE,
        ZDB_API_ENTRY,
        ZDB_API_UP_TO_DATE,
        ZDB_API_BUFFER,
        ZDB_API_NOT_FOUND,
        ZDB_API_DELETED,
        ZDB_API_INTERNAL_ERROR,
        ZDB_API_TRUE,
        ZDB_API_FALSE,
        ZDB_API_INSERT_DENIED,

        ZDB_API_ITEMS_TOTAL  // last element

    } zdb_api_type_t;

    typedef struct zdb_api_t {
        zdb_api_type_t status;
        void *payload;

    } zdb_api_t;

    typedef struct zdb_api_buffer_t {
        uint8_t *payload;
        size_t size;

    } zdb_api_buffer_t;

    typedef struct zdb_api_entry_t {
        zdb_api_buffer_t key;
        zdb_api_buffer_t payload;

    } zdb_api_entry_t;

    zdb_api_t *zdb_api_set(namespace_t *ns, void *key, size_t ksize, void *payload, size_t psize);
    zdb_api_t *zdb_api_get(namespace_t *ns, void *key, size_t ksize);
    zdb_api_t *zdb_api_exists(namespace_t *ns, void *key, size_t ksize);
    zdb_api_t *zdb_api_check(namespace_t *ns, void *key, size_t ksize);
    zdb_api_t *zdb_api_del(namespace_t *ns, void *key, size_t ksize);

    char *zdb_api_debug_type(zdb_api_type_t type);
    void zdb_api_reply_free(zdb_api_t *reply);
#endif
