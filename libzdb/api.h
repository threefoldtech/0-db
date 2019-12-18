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

    // index helper
    char *zdb_index_date(uint32_t epoch, char *target, size_t length);

    // index loader
    void zdb_index_set_id(index_root_t *root, uint64_t fileid);

    int zdb_index_open_readonly(index_root_t *root, uint16_t fileid);
    int zdb_index_open_readwrite(index_root_t *root, uint16_t fileid);
    void zdb_index_close(index_root_t *zdbindex);

    index_root_t *zdb_index_init_lazy(zdb_settings_t *settings, char *indexdir, void *namespace);
    index_root_t *zdb_index_init(zdb_settings_t *settings, char *indexdir, void *namespace, index_branch_t **branches);
    uint64_t zdb_index_availity_check(index_root_t *root);

    // index header validity
    index_header_t *zdb_index_descriptor_load(index_root_t *root);
    index_header_t *zdb_index_descriptor_validate(index_header_t *header, index_root_t *root);

    // low level index
    index_item_t *zdb_index_raw_fetch_entry(index_root_t *root);
    off_t zdb_index_raw_offset(index_root_t *root);
    uint64_t zdb_index_next_id(index_root_t *root);

    // internal checksum
    uint32_t zdb_checksum_crc32(const uint8_t *bytes, ssize_t length);

    // data loader
    data_root_t *zdb_data_init_lazy(zdb_settings_t *settings, char *datapath, uint16_t dataid);
    int zdb_data_open_readonly(data_root_t *root);

    data_header_t *zdb_data_descriptor_load(data_root_t *root);
    data_header_t *zdb_data_descriptor_validate(data_header_t *header, data_root_t *root);
#endif
