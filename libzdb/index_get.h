#ifndef ZDB_INDEX_GET_H
    #define ZDB_INDEX_GET_H

    index_entry_t *index_get(index_root_t *index, void *id, uint8_t idlength);
    index_item_t *index_raw_fetch_entry(index_root_t *root);
#endif
