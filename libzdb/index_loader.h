#ifndef __ZDB_INDEX_LOADER_H
    #define __ZDB_INDEX_LOADER_H

    // initialize index header file
    index_header_t index_initialize(int fd, uint16_t indexid, index_root_t *root);

    // initialize the whole index system
    index_root_t *index_init(zdb_settings_t *settings, char *indexdir, void *namespace, index_branch_t **branches);
    index_root_t *index_init_lazy(zdb_settings_t *settings, char *indexdir, void *namespace);

    // internal functions
    void index_internal_load(index_root_t *root);
    void index_internal_allocate_single();

    // sanity check
    uint64_t index_availity_check(index_root_t *root);
    index_header_t *index_descriptor_load(index_root_t *root);
    index_header_t *index_descriptor_validate(index_header_t *header, index_root_t *root);

    // gracefully clean everything
    void index_delete_files(index_root_t *root);
    void index_destroy(index_root_t *root);
    void index_destroy_global();
#endif
