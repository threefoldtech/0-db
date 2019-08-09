#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include "libzdb.h"
#include "libzdb_private.h"

// perform a binary search on the seqmap to get
// the fileid back based on the index mapped with fileid
static index_seqmap_t *index_seqmap_from_seq(index_seqid_t *seqid, uint32_t id) {
    int lower = 0;
    int higher = seqid->length;

    // binary search
    while(lower < higher) {
        int mid = (lower + higher) / 2;

        // exact match
        if(id == seqid->seqmap[mid].seqid)
            return &seqid->seqmap[mid];

        // lower side
        if(id < seqid->seqmap[mid].seqid) {
            higher = mid;
            continue;
        }

        // are we on the right interval
        if(lower + 1 >= seqid->length)
            return &seqid->seqmap[lower];

        if(seqid->seqmap[lower].seqid < id && seqid->seqmap[lower + 1].seqid > id)
            return &seqid->seqmap[lower];

        // it's on the higher side
        lower = mid;
    }

    return &seqid->seqmap[lower];
}

index_seqmap_t *index_fileid_from_seq(index_root_t *root, uint32_t seqid) {
    index_seqmap_t *seqmap = index_seqmap_from_seq(root->seqid, seqid);
    zdb_debug("[+] index: seqmap: resolving %d -> file %u\n", seqid, seqmap->fileid);

    return seqmap;
}

void index_seqid_push(index_root_t *root, uint32_t id, uint16_t indexid) {
    zdb_debug("[+] index seq: mapping id %u to file %u\n", id, indexid);

    if(root->seqid->length + 1 == root->seqid->allocated) {
        // growing up the vector
        root->seqid->allocated += 1024;
        zdb_debug("[+] index seq: growing up vector of files (%u entries)\n", root->seqid->allocated);

        if(!(root->seqid->seqmap = realloc(root->seqid->seqmap, sizeof(index_seqmap_t) * root->seqid->allocated)))
            zdb_diep("index seqmap: realloc");
    }

    root->seqid->seqmap[root->seqid->length].fileid = indexid;
    root->seqid->seqmap[root->seqid->length].seqid = id;
    root->seqid->length += 1;
}

size_t index_seq_offset(uint32_t relative) {
    // skip index header
    size_t offset = sizeof(index_header_t);

    // index is linear like this
    // [header][obj-1][obj-2][obj-3][...]
    //
    // object X offset can be found by computing
    // size of each entry, in direct mode, keys are
    // always fixed-length
    //
    //                       each entry            fixed-key-length
    offset += (relative * (sizeof(index_item_t) + sizeof(uint32_t)));

    return offset;
}

void index_seqid_dump(index_root_t *root) {
    for(uint16_t i = 0; i < root->seqid->length; i++) {
        index_seqmap_t *item = &root->seqid->seqmap[i];
        printf("[+] index seq: seqid %d -> file %d\n", item->seqid, item->fileid);
    }
}


