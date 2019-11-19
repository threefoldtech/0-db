#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <inttypes.h>
#include "libzdb.h"
#include "libzdb_private.h"

static index_entry_t *index_get_handler_memkey(index_root_t *index, void *id, uint8_t idlength) {
    return index_entry_get(index, id, idlength);
}

static index_entry_t *index_get_handler_sequential(index_root_t *index, void *id, uint8_t idlength) {
    if(idlength != sizeof(uint32_t)) {
        zdb_debug("[-] index: sequential get: invalid key length (%u <> %ld)\n", idlength, sizeof(uint32_t));
        return NULL;
    }

    // converting key into binary format
    uint32_t key;
    memcpy(&key, id, sizeof(uint32_t));

    // resolving key into file id
    index_seqmap_t *seqmap = index_fileid_from_seq(index, key);

    // resolving relative offset
    uint32_t relative = key - seqmap->seqid;
    uint32_t offset = index_seq_offset(relative);

    // reading index on disk
    index_item_t *item;

    if(!(item = index_item_get_disk(index, seqmap->fileid, offset, sizeof(uint32_t))))
        return NULL;

    memcpy(index_reusable_entry->id, item->id, item->idlength);
    index_reusable_entry->idlength = item->idlength;
    index_reusable_entry->offset = item->offset;
    index_reusable_entry->dataid = item->dataid;
    index_reusable_entry->indexid = seqmap->fileid;
    index_reusable_entry->flags = item->flags;
    index_reusable_entry->idxoffset = offset;
    index_reusable_entry->crc = item->crc;
    index_reusable_entry->parentid = item->parentid;
    index_reusable_entry->parentoff = item->parentoff;
    index_reusable_entry->timestamp = item->timestamp;
    index_reusable_entry->length = item->length;

    // cleaning intermediate object
    free(item);

    return index_reusable_entry;
}

static index_entry_t * (*index_get_handlers[])(index_root_t *root, void *id, uint8_t idlength) = {
    index_get_handler_memkey,     // key-value mode
    index_get_handler_sequential, // incremental mode
    index_get_handler_sequential, // direct-key mode (not used anymore)
    index_get_handler_sequential, // fixed block mode (not implemented yet)
};

index_entry_t *index_get(index_root_t *index, void *id, uint8_t idlength) {
    index_entry_t *entry;

    zdb_debug("[+] index: get: lookup key: ");
    zdb_debughex(id, idlength);
    zdb_debug("\n");

    if(!(entry = index_get_handlers[index->mode](index, id, idlength))) {
        zdb_debug("[-] index: get: key not found\n");
        return NULL;
    }

    // key found but deleted
    if(entry->flags & INDEX_ENTRY_DELETED) {
        zdb_debug("[-] index: get: key requested deleted\n");
        return NULL;
    }

    return entry;
}

//
// low level helper
//
index_item_t *index_raw_fetch_entry(index_root_t *root) {
    uint8_t idlength;
    index_item_t *entry = NULL;

    if(read(root->indexfd, &idlength, sizeof(idlength)) != sizeof(idlength))
        return NULL;

    // we have the length of the key
    ssize_t entrylength = sizeof(index_item_t) + idlength;
    if(!(entry = malloc(entrylength)))
        zdb_diep("index_raw_fetch_entry: malloc");

    // rollback the 1 byte read for the id length
    lseek(root->indexfd, -1, SEEK_CUR);

    if(read(root->indexfd, entry, entrylength) != entrylength)
        zdb_diep("index header read failed");

    return entry;
}
