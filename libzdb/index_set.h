#ifndef ZDB_INDEX_SET_H
    #define ZDB_INDEX_SET_H

    typedef struct index_set_t {
        index_entry_t *entry;
        void *id;

    } index_set_t;

    index_entry_t *index_set(index_root_t *root, index_set_t *new, index_entry_t *existing);
    index_entry_t *index_set_memory(index_root_t *root, void *id, index_entry_t *entry);

    index_item_t *index_item_from_set(index_root_t *root, index_set_t *set);

    // internal index append functions
    int index_append_entry_on_disk(index_root_t *root, index_set_t *set);
#endif
