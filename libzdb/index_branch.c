#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include "libzdb.h"
#include "libzdb_private.h"

#define INDEX_HASH_SUB  1
#define INDEX_HASH_LIST 2

#define BITS_PER_ROWS     4    // 4 bits per entry (0x00 -> 0x0f)
#define KEY_LENGTH        20   // using crc32 but only using 20 bits
#define DEEP_LEVEL        KEY_LENGTH / BITS_PER_ROWS  // 5 levels (20 bits total, 4 bits per entry)
#define ENTRIES_PER_ROWS  1 << BITS_PER_ROWS          // 0x00 -> 0x0f = 16

//
// CRC32 => 0x10320af
//       => 0x10320..    # we only use 20 bits
//
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// |0|1|2|3|4|5|6|7|8|9|A|B|C|D|E|F|
// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    ^                                            0x1xxxxxxx
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//    |0|1|2|3|4|5|6|7|8|9|A|B|C|D|E|F|
//    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     ^                                           0x10xxxxxx
//     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     |0|1|2|3|4|5|6|7|8|9|A|B|C|D|E|F|
//     +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//            ^                                    0x103xxxxx
//            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//            |0|1|2|3|4|5|6|7|8|9|A|B|C|D|E|F|
//            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//                 ^                               0x1032xxxx
//                 ...
//
// when the last level is reached, list object point to the
// head of a linked-list of entries

index_hash_t *index_hash_new(int type) {
    index_hash_t *root;

    if(!(root = calloc(sizeof(index_hash_t), 1)))
        zdb_diep("index: hash: root calloc");

    if(type == INDEX_HASH_SUB) {
        root->type = INDEX_HASH_SUB;
        if(!(root->sub = calloc(sizeof(index_hash_t **), ENTRIES_PER_ROWS)))
            zdb_diep("index: hash: sub calloc");
    }

    if(type == INDEX_HASH_LIST)
        root->type = INDEX_HASH_LIST;

    return root;
}

index_hash_t *index_hash_init() {
    return index_hash_new(INDEX_HASH_SUB);
}

void *index_hash_push(index_hash_t *root, uint32_t lookup, index_entry_t *entry) {
    // start with mask 0x0000000f (with 4 bits per rows)
    uint32_t shift = ~(0xffffffff << BITS_PER_ROWS);

    // same algorythm than lookup, but with allocation
    for(int i = 0; i < DEEP_LEVEL; i++) {
        unsigned int mask = (lookup & shift);
        unsigned int check = mask >> (i * BITS_PER_ROWS);

        if(root->sub[check] == NULL) {
            if(i < DEEP_LEVEL - 1)
                root->sub[check] = index_hash_new(INDEX_HASH_SUB);

            if(i == DEEP_LEVEL - 1)
                root->sub[check] = index_hash_new(INDEX_HASH_LIST);
        }

        if(i == DEEP_LEVEL - 1) {
            entry->next = root->sub[check]->list;
            root->sub[check]->list = entry;

            return entry;
        }

        root = root->sub[check];
        shift <<= BITS_PER_ROWS;
    }

    // insertion failed, should never happen
    return NULL;

}

static index_hash_t *index_hash_lookup_member(index_hash_t *root, uint32_t lookup) {
    // BITS_PER_ROWS specifies how many bits we use to compare each level
    // we need to use a mask we shift for each level, we hardcode maximum
    // to 32 bits mask
    //
    // starting from 0xffffffff (all bits sets)
    //
    // with 4 bits:
    //     Shifting with amount of bits: 0xfffffff0
    //     Then negate that            : 0x0000000f
    //
    // with 16 bits:
    //     Shifting with amount of bits: 0xffff0000
    //     Then negate that            : 0x0000ffff

    // start with mask 0x0000000f (with 4 bits per rows)
    uint32_t shift = ~(0xffffffff << BITS_PER_ROWS);

    // printf(">> %x\n", lookup);

    for(int i = 0; i < DEEP_LEVEL; i++) {
        unsigned int mask = (lookup & shift);
        unsigned int check = mask >> (i * BITS_PER_ROWS);

        if(root->sub[check] == NULL)
            return NULL;

        root = root->sub[check];
        shift <<= BITS_PER_ROWS;
    }

    return root;
}

index_entry_t *index_hash_lookup(index_hash_t *root, uint32_t lookup) {
    index_hash_t *member;

    if(!(member = index_hash_lookup_member(root, lookup)))
        return NULL;

    // point to the head of the list
    return member->list;
}

index_entry_t *index_hash_remove(index_hash_t *root, uint32_t lookup, index_entry_t *entry) {
    index_hash_t *member = index_hash_lookup_member(root, lookup);
    if(!member)
        return NULL;

    // entry is the list head, replace
    // head with next entry and we are done
    if(member->list == entry) {
        member->list = entry->next;
        return entry;
    }

    // looking for the entry in the list
    index_entry_t *previous = member->list;
    while(previous->next != entry)
        previous = previous->next;

    // update linked list
    previous->next = entry->next;

    return entry;
}

// call user function pointer (with user argument) for
// each entries available on the index, the order follow memory
// order and is not related to entries
int index_hash_walk(index_hash_t *root, int (*callback)(index_entry_t *, void *), void *userptr) {
    index_entry_t *entry;
    int value;

    for(int i = 0; i < ENTRIES_PER_ROWS; i++) {
        // ignore unallocated sub
        if(!root->sub[i])
            continue;

        if(root->sub[i]->type == INDEX_HASH_LIST) {
            for(entry = root->sub[i]->list; entry; entry = entry->next) {
                if((value = callback(entry, userptr)) != 0) {
                    // callback interruption
                    return value;
                }
            }
        }

        if(root->sub[i]->type == INDEX_HASH_SUB) {
            if((value = index_hash_walk(root->sub[i], callback, userptr)) != 0) {
                // callback interruption
                return value;
            }
        }
    }

    return 0;
}

// compute statistics on index entries and size
static index_hash_stats_t index_hash_stats_level(index_hash_t *root) {
    index_hash_stats_t stats = {
        .subs = 0,
        .subsubs = 0,
        .entries = 0,
        .max_entries = 0,
        .lists = 0,
        .entries_size = 0,
        .ids_size = 0,
    };

    for(int i = 0; i < ENTRIES_PER_ROWS; i++) {
        if(root->sub[i]) {
            stats.subs += 1;

            if(root->sub[i]->type == INDEX_HASH_LIST) {
                size_t localent = 0;
                stats.lists += 1;

                for(index_entry_t *entry = root->sub[i]->list; entry; entry = entry->next) {
                    stats.entries_size += sizeof(index_entry_t) + entry->idlength;
                    stats.ids_size += entry->idlength;
                    localent += 1;
                }

                if(localent > stats.max_entries)
                    stats.max_entries = localent;

                stats.entries += localent;
            }

            if(root->sub[i]->type == INDEX_HASH_SUB) {
                stats.subsubs += 1;
                index_hash_stats_t extra = index_hash_stats_level(root->sub[i]);

                stats.subs += extra.subs;
                stats.subsubs += extra.subsubs;
                stats.entries += extra.entries;
                stats.lists += extra.lists;
                stats.entries_size += extra.entries_size;
                stats.ids_size += extra.ids_size;

                if(extra.max_entries > stats.max_entries)
                    stats.max_entries = extra.max_entries;
            }
        }
    }

    return stats;
}

void index_hash_stats(index_hash_t *root) {
    index_hash_stats_t stats = index_hash_stats_level(root);
    size_t subs_size = stats.subs * sizeof(index_hash_t);
    size_t lists_size = stats.lists * sizeof(index_entry_t *);
    size_t arrays_size = stats.subsubs * sizeof(index_hash_t **) * ENTRIES_PER_ROWS;

    zdb_debug("[+] index: metrics: subs alloc : %lu\n", stats.subs);
    zdb_debug("[+] index: metrics: lists alloc: %lu\n", stats.lists);
    zdb_debug("[+] index: metrics: subsubs    : %lu\n", stats.subsubs);
    zdb_verbose("[+] index: metrics: entries    : %lu\n", stats.entries);
    zdb_debug("[+] index: metrics: max entries: %lu\n", stats.max_entries);
    zdb_verbose("[+] index: metrics: items size : %lu (%.2f MB)\n", stats.entries_size, MB(stats.entries_size));
    zdb_verbose("[+] index: metrics: items ids  : %lu (%.2f MB)\n", stats.ids_size, MB(stats.ids_size));
    zdb_verbose("[+] index: metrics: subs size  : %lu (%.2f MB)\n", subs_size, MB(subs_size));
    zdb_verbose("[+] index: metrics: lists size : %lu (%.2f MB)\n", lists_size, MB(lists_size));
    zdb_verbose("[+] index: metrics: subs array : %lu (%.2f MB)\n", arrays_size, MB(arrays_size));

    if(stats.lists) {
        zdb_debug("[+] index: metrics: avg entries: %lu\n", stats.entries / stats.lists);
    }

    size_t total = stats.entries_size + subs_size + lists_size + arrays_size;

    zdb_verbose("[+] index: metrics: total size : %lu (%.2f MB)\n", total, MB(total));
}

static void index_hash_free_list(index_entry_t *head) {
    index_entry_t *entry = head;

    while(entry) {
        // copy current entry and saving next address
        // before freeing the object
        index_entry_t *current = entry;
        entry = current->next;

        // free object
        free(current);
    }
}

void index_hash_free(index_hash_t *root) {
    for(int i = 0; i < ENTRIES_PER_ROWS; i++) {
        if(root->sub[i]) {
            // clean the linked list
            if(root->sub[i]->type == INDEX_HASH_LIST) {
                index_hash_free_list(root->sub[i]->list);
                free(root->sub[i]);
                continue;
            }

            if(root->sub[i]->type == INDEX_HASH_SUB)
                index_hash_free(root->sub[i]);
        }
    }

    free(root->sub);
    free(root);
}

