#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include "zerodb.h"
#include "index.h"
#include "index_branch.h"

// maximum allowed branch in memory
//
// this settings is mainly the most important to
// determine the keys lookup time
//
// the more bits you allows here, the more buckets
// can be used for lookup without collision
//
// the index works like a hash-table and uses crc32 'hash'
// algorithm, the result of the crc32 is used to point to
// the bucket, but using a full 32-bits hashlist would
// consume more than (2^32 * 8) bytes of memory (on 64-bits)
//
// the default settings sets this to 24 bits, which allows
// 16 millions direct entries, collisions uses linked-list
//
// makes sur mask and amount of branch are always in relation
// use 'index_set_buckets_bits' to be sure
uint32_t buckets_branches = (1 << 24);
uint32_t buckets_mask = (1 << 24) - 1;

// WARNING: this doesn't resize anything, you should calls this
//          only before initialization
int index_set_buckets_bits(uint8_t bits) {
    buckets_branches = 1 << bits;
    buckets_mask = (1 << bits) - 1;

    return buckets_branches;
}

//
// index branch
// this implementation use a lazy load of branches
// this allows us to use lot of branch (buckets_branches) in this case)
// without consuming all the memory if we don't need it
//
index_branch_t *index_branch_init(index_root_t *root, uint32_t branchid) {
    // debug("[+] initializing branch id 0x%x\n", branchid);

    root->branches[branchid] = malloc(sizeof(index_branch_t));
    index_branch_t *branch = root->branches[branchid];

    branch->length = 0;
    branch->last = NULL;
    branch->list = NULL;

    return branch;
}

void index_branch_free(index_root_t *root, uint32_t branchid) {
    // this branch was not allocated
    if(!root->branches[branchid])
        return;

    index_entry_t *entry = root->branches[branchid]->list;
    index_entry_t *next = NULL;

    // deleting branch content by
    // iterate over the linked-list
    for(; entry; entry = next) {
        next = entry->next;
        free(entry);
    }

    // deleting branch
    free(root->branches[branchid]);
}

// returns branch from rootindex, if branch is not allocated yet, returns NULL
// useful for any read on the index in memory
index_branch_t *index_branch_get(index_root_t *root, uint32_t branchid) {
    if(!root->branches)
        return NULL;

    return root->branches[branchid];
}

// returns branch from rootindex, if branch doesn't exists, it will be allocated
// (useful for any write in the index in memory)
index_branch_t *index_branch_get_allocate(index_root_t *root, uint32_t branchid) {
    if(!root->branches[branchid])
        return index_branch_init(root, branchid);

    debug("[+] branch: exists: %lu entries\n", root->branches[branchid]->length);
    return root->branches[branchid];
}

// append an entry (item) to the memory list
// since we use a linked-list, the logic of appending
// only occures here
//
// if there is no index, we just skip the appending
index_entry_t *index_branch_append(index_root_t *root, uint32_t branchid, index_entry_t *entry) {
    index_branch_t *branch;

    if(!root->branches)
        return NULL;

    // grabbing the branch
    branch = index_branch_get_allocate(root, branchid);
    branch->length += 1;

    // adding this item and pointing previous last one
    // to this new one
    if(!branch->list)
        branch->list = entry;

    if(branch->last)
        branch->last->next = entry;

    branch->last = entry;
    entry->next = NULL;

    return entry;
}

// remove one entry on this branch
// since it's a linked-list, we need to know which entry was the previous one
// we use a single-direction linked-list
//
// removing an entry from the list don't free this entry, is just re-order
// list to keep it coherent
index_entry_t *index_branch_remove(index_branch_t *branch, index_entry_t *entry, index_entry_t *previous) {
    // removing the first entry
    if(branch->list == entry)
        branch->list = entry->next;

    // skipping this entry, linking next from previous
    // to our next one
    if(previous)
        previous->next = entry->next;

    // if our entry was the last one
    // the new last one is the previous one
    if(branch->last == entry)
        branch->last = previous;

    branch->length -= 1;

    return entry;
}
