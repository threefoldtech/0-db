#ifndef __ZDB_INDEX_LOADER_H
    #define __ZDB_INDEX_LOADER_H

    // initialize index header file
    index_header_t index_initialize(int fd, uint16_t indexid, index_root_t *root);

    // initialize the whole index system
    index_root_t *index_init(zdb_settings_t *settings, char *indexdir, void *namespace, index_branch_t **branches);

    // gracefully clean everything
    void index_delete_files(index_root_t *root);
    void index_destroy(index_root_t *root);
    void index_destroy_global();
#endif
