#ifndef __ZDB_INDEX_BRANCH_H
    #define __ZDB_INDEX_BRANCH_H

    // buckets
    extern uint32_t buckets_branches;
    extern uint32_t buckets_mask;

    int index_set_buckets_bits(uint8_t bits);
    index_branch_t **index_buckets_init();

    // initializers
    index_branch_t *index_branch_init(index_branch_t **branches, uint32_t branchid);
    void index_branch_free(index_branch_t **branches, uint32_t branchid);

    // accessors
    index_branch_t *index_branch_get(index_branch_t **branches, uint32_t branchid);
    index_branch_t *index_branch_get_allocate(index_branch_t **branches, uint32_t branchid);
    index_entry_t *index_branch_append(index_branch_t **branches, uint32_t branchid, index_entry_t *entry);
    index_entry_t *index_branch_remove(index_branch_t *branch, index_entry_t *entry, index_entry_t *previous);
    index_entry_t *index_branch_get_previous(index_branch_t *branch, index_entry_t *entry);
#endif
