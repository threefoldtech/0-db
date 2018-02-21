#ifndef __ZDB_INDEX_BRANCH_H
    #define __ZDB_INDEX_BRANCH_H

    // buckets
    extern uint32_t buckets_branches;
    extern uint32_t buckets_mask;

    int index_set_buckets_bits(uint8_t bits);

    // initializers
    index_branch_t *index_branch_init(index_root_t *root, uint32_t branchid);
    void index_branch_free(index_root_t *root, uint32_t branchid);

    // accessors
    index_branch_t *index_branch_get(index_root_t *root, uint32_t branchid);
    index_branch_t *index_branch_get_allocate(index_root_t *root, uint32_t branchid);
    index_entry_t *index_branch_append(index_root_t *root, uint32_t branchid, index_entry_t *entry);
#endif
