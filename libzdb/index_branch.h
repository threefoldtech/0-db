#ifndef __ZDB_INDEX_BRANCH_H
    #define __ZDB_INDEX_BRANCH_H

    // initializers
    index_hash_t *index_hash_init();
    index_hash_t *index_hash_new(int type);

    // cleaner
    void index_hash_free(index_hash_t *root);

    // list manipulation
    void *index_hash_push(index_hash_t *root, uint32_t lookup, index_entry_t *entry);
    index_entry_t *index_hash_lookup(index_hash_t *root, uint32_t lookup);
    index_entry_t *index_hash_remove(index_hash_t *root, uint32_t lookup, index_entry_t *entry);

    // inspection
    int index_hash_walk(index_hash_t *root, int (*callback)(index_entry_t *, void *), void *userptr);

    // statistics
    void index_hash_stats(index_hash_t *root);

#endif
