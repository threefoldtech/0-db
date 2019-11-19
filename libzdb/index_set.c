#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <inttypes.h>
#include <time.h>
#include "libzdb.h"
#include "libzdb_private.h"

//
// in memory and on disk index representation
//
// in userkey mode:
//   on each insertion a of new key, a new entry is added on the index on disk,
//   this entry contains latest up-to-date metadata
//
//   if the inserted key is an update of an existing key, the previous entry on the
//   index is overwritten, just to change the flag to DELETED, using this, we can
//   compact data later and when replaying index on reload, we know we need to
//   skip this entry
//
//   moreover, on update, the new keys keep pointer to the previous key
//   to enable history feature (to find previous entry)
//
// in sequential mode:
//   this mode works mostly like the userkey mode, except the index is not time
//   linerar updated, let explain why:
//     in sequential mode, the sequence id returned when inserting a new key
//     is a location-relative identifier, the id is used to find the data on
//     the disk
//
//     if you want to update the key 0, since the id 0 will be used to find the
//     data on the disk, we need to update the metadata at 'relative-position 0'
//
//   in order to keep history and all the feature we have, we need to do some
//   tricky stuff but not so dirty:
//     when inserting new data, nothing special is made, except adding a new
//     entry on the index, the location will be used as returned key
//
//     if we update a key, we need to keep the chain for history and the flags
//     to know what to do on compaction or replay, in order to supports that,
//     we simply add a *new* entry on index, but already flagged as deleted,
//     this entry will in fact be a copy of the original key, flagged as deleted
//     and the original key (the one at the right location) will be updated
//     with latest up-to-date metadata *and* history pointers updated
//
//     like this, the history is kept and when replaying the index, we know
//     if the key is relevant or not (deleted or not)
//

// special sequential function
int index_seq_overwrite(index_root_t *root, index_set_t *set) {
    index_entry_t *entry = set->entry;
    index_item_t original;
    int fd;

    // compute this object size on disk
    size_t entrylength = sizeof(index_item_t) + entry->idlength;

    // converting key into binary format
    uint32_t key;
    memcpy(&key, set->id, sizeof(uint32_t));

    // resolving key into file id
    index_seqmap_t *seqmap = index_fileid_from_seq(root, key);

    // resolving relative offset
    uint32_t relative = key - seqmap->seqid;
    uint32_t offset = index_seq_offset(relative);

    // (re-)open the expected index file, in read-write mode
    if((fd = index_open_readwrite_oneshot(root, seqmap->fileid)) < 0)
        return 1;

    // jump to the right offset for this entry
    zdb_debug("[+] index: sequential: overwritting at %u/%u\n", seqmap->fileid, offset);
    lseek(fd, offset, SEEK_SET);

    index_item_t *item = index_item_from_set(root, set);

    // reading original entry
    if(read(fd, &original, sizeof(index_item_t)) != sizeof(index_item_t)) {
        zdb_warnp("index_seq_overwrite re-read");
        close(fd);
        return 1;
    }

    // important fix: do not update previous field
    // keeping original previous offset
    item->previous = original.previous;

    // overwrite the key
    lseek(fd, offset, SEEK_SET);

    if(write(fd, item, entrylength) != (ssize_t) entrylength) {
        zdb_warnp("index_seq_overwrite re-write");
        close(fd);
        return 1;
    }

    close(fd);

    return 0;
}


//
// common writer
//
index_item_t *index_item_from_set(index_root_t *root, index_set_t *set) {
    index_entry_t *entry = set->entry;

    // copying entry object to item object
    memcpy(index_transition->id, set->id, entry->idlength);
    index_transition->idlength = entry->idlength;
    index_transition->offset = entry->offset;
    index_transition->length = entry->length;
    index_transition->flags = entry->flags;
    index_transition->dataid = root->indexid; // WARNING: check this
    index_transition->timestamp = entry->timestamp;
    index_transition->previous = root->previous;
    index_transition->parentid = entry->parentid;
    index_transition->parentoff = entry->parentoff;
    index_transition->crc = entry->crc;
    index_transition->timestamp = entry->timestamp;

    return index_transition;
}

int index_append_entry_on_disk(index_root_t *root, index_set_t *set) {
    index_entry_t *entry = set->entry;
    off_t curoffset = lseek(root->indexfd, 0, SEEK_END);
    size_t entrylength = sizeof(index_item_t) + entry->idlength;

    zdb_debug("[+] index: writing entry on disk (%lu bytes)\n", entrylength);

    // updating entry with the real offset
    // we used to insert this object
    entry->idxoffset = curoffset;
    index_item_t *item = index_item_from_set(root, set);

    // updating global previous
    root->previous = curoffset;

    // writing data on the disk
    if(!index_write(root->indexfd, item, entrylength, root)) {
        zdb_verbosep("index_append_entry_on_disk", "cannot write index entry on disk");

        // it's easier to flag the entry as deleted than
        // removing it from the list
        entry->flags |= INDEX_ENTRY_DELETED;
        return 1;
    }

    return 0;
}



//
// insertion
// everything to do when a set is done on a key which is not already
// existing at all (or was deleted, anyway it's not existing for us)
//

index_entry_t *index_insert_memory_handler_memkey(index_root_t *root, index_set_t *set) {
    index_entry_t *new = set->entry;
    index_entry_t *entry;

    // calloc will ensure any unset fields (eg: flags) are zero
    size_t entrysize = sizeof(index_entry_t) + new->idlength;
    if(!(entry = calloc(entrysize, 1)))
        return NULL;

    memcpy(entry->id, set->id, new->idlength);
    entry->idlength = new->idlength;
    entry->namespace = root->namespace;
    entry->offset = new->offset;
    entry->length = new->length;
    entry->dataid = root->indexid; // WARNING: check this
    entry->indexid = new->indexid; // WARNING: check this
    entry->idxoffset = new->idxoffset;
    entry->flags = new->flags;
    entry->crc = new->crc;
    entry->timestamp = new->timestamp;
    entry->parentid = new->parentid;
    entry->parentoff = new->parentoff;

    uint32_t branchkey = index_key_hash(entry->id, entry->idlength);

    // commit entry into memory
    index_branch_append(root->branches, branchkey, entry);

    // update statistics (if the key exists)
    // maybe it doesn't exists if it comes from a replay
    root->entries += 1;
    root->datasize += new->length;
    root->indexsize += entrysize;

    // update next entry id
    root->nextentry += 1;
    root->nextid += 1;

    return entry;
}

index_entry_t *index_insert_memory_handler_sequential(index_root_t *root, index_set_t *set) {
    index_entry_t *new = set->entry;

    // update statistics (if the key exists)
    // maybe it doesn't exists if it comes from a replay
    root->entries += 1;
    root->datasize += new->length;

    // update next entry id
    root->nextentry += 1;
    root->nextid += 1;

    return new;
}

//
// updating
// everything related to an update (doing a set on a key already existing)
// all of this work take care about linking nodes for history, etc.
//
index_entry_t *index_update_memory_handler_memkey(index_root_t *root, index_set_t *set, index_entry_t *exists) {
    index_entry_t *new = set->entry;

    zdb_debug("[+] index: updating current entry in memory\n");

    // update statistics
    root->datasize -= exists->length;
    root->datasize += new->length;

    // updating parent id and parent offset
    // to the previous item itself, which
    // will be used to keep track of the history
    exists->parentid = exists->dataid;
    exists->parentoff = exists->idxoffset;

    // re-use existing entry
    exists->length = new->length;
    exists->offset = new->offset;
    exists->flags = new->flags;
    exists->dataid = root->indexid; // WARNING: check this
    exists->idxoffset = new->idxoffset;
    exists->crc = new->crc;
    exists->timestamp = new->timestamp;

    return exists;
}

index_entry_t *index_update_memory_handler_sequential(index_root_t *root, index_set_t *set, index_entry_t *exists) {
    index_entry_t *new = set->entry;

    root->nextentry += 1;
    root->nextid += 1;

    // nothing to do if the key is deleted
    if(index_entry_is_deleted(new))
        return new;

    // key is not deleted, let's update statistics
    if(!exists) {
        // blindly if exists is NULL (that means we comes from
        // 'index_set_memory' and we are not really doing some update
        // but more a replay from the index_loader
        root->datasize += new->length;
        root->entries += 1;
        return new;
    }

    // we have a previous key, this is a real
    // update and we need to reflect that on the statistics
    // there are no new entry, since it's an update
    root->datasize -= exists->length;
    root->datasize += new->length;

    return new;
}

// public disk and memory part
index_entry_t *index_update_entry_memkey(index_root_t *root, index_set_t *set, index_entry_t *previous) {
    zdb_debug("[+] index: flagging previous key as deleted, on disk\n");
    index_entry_delete_disk(root, previous);

    // update memory state
    index_entry_t *entry = index_update_memory_handler_memkey(root, set, previous);

    index_set_t updated = {
        .entry = entry,
        .id = set->id,
    };

    // append the data on the disk
    if(index_append_entry_on_disk(root, &updated)) {
        zdb_debug("[-] index: update entry failed: could not write on disk\n");
        return NULL;
    }

    return entry;
}

index_entry_t *index_update_entry_sequential(index_root_t *root, index_set_t *set, index_entry_t *previous) {
    zdb_debug("[+] index: update on sequential keys, duplicating key flagged\n");

    // mark previous as deleted, and writing this object on the index
    // this will add a *new* entry on the index file, and we will use this
    // as reference to update the first one, to keep history and so one
    previous->flags |= INDEX_ENTRY_DELETED;

    index_set_t updated = {
        .entry = previous,
        .id = set->id,
    };

    if(index_append_entry_on_disk(root, &updated)) {
        zdb_debug("[-] index: update entry failed: could not write on disk\n");
        return NULL;
    }

    // overwrite original object
    // with the latest data (the new one, basicly)
    set->entry->parentid = root->indexid;
    set->entry->parentoff = previous->idxoffset;
    index_seq_overwrite(root, set);

    // since we added a new entry on the index, the next id needs
    // to be incremented to skip this position in the futur
    root->nextid += 1;
    root->nextentry += 1;

    return set->entry;
}

static index_entry_t * (*index_update_entry[])(index_root_t *root, index_set_t *new, index_entry_t *previous) = {
    index_update_entry_memkey,     // key-value mode
    index_update_entry_sequential, // incremental mode
    index_update_entry_sequential, // direct-key mode (not used anymore)
    index_update_entry_sequential, // fixed block mode (not implemented yet)
};



index_entry_t *index_insert_entry_memkey(index_root_t *root, index_set_t *set) {
    // ensure flags are empty
    set->entry->flags = 0;

    // append the data on the disk
    if(index_append_entry_on_disk(root, set)) {
        zdb_debug("[-] index: add new entry failed: could not write on disk\n");
        return NULL;
    }

    // update memory system
    return index_insert_memory_handler_memkey(root, set);
}

index_entry_t *index_insert_entry_sequential(index_root_t *root, index_set_t *set) {
    // ensure flags are empty
    set->entry->flags = 0;

    // append the data on the disk
    if(index_append_entry_on_disk(root, set)) {
        zdb_debug("[-] index: add new entry failed: could not write on disk\n");
        return NULL;
    }

    // update memory system
    return index_insert_memory_handler_sequential(root, set);
}

static index_entry_t * (*index_insert_entry[])(index_root_t *root, index_set_t *new) = {
    index_insert_entry_memkey,     // key-value mode
    index_insert_entry_sequential, // incremental mode
    index_insert_entry_sequential, // direct-key mode (not used anymore)
    index_insert_entry_sequential, // fixed block mode (not implemented yet)
};

//
// common function which does the distinction between
// update and insertion
//
// theses are public function which should be used
//


// do the set effet:
//  - update if already exists
//  - insert new if not existing
index_entry_t *index_set(index_root_t *root, index_set_t *new, index_entry_t *existing) {
    if(existing) {
        zdb_debug("[+] index: set: updating existing entry\n");
        return index_update_entry[root->mode](root, new, existing);
    }

    zdb_debug("[+] index: set: inserting new entry\n");
    return index_insert_entry[root->mode](root, new);
}

index_entry_t *index_set_memory(index_root_t *root, void *id, index_entry_t *entry) {
    index_entry_t *existing;
    index_set_t setter = {
        .entry = entry,
        .id = id,
    };

    // in sequential mode, there is no memory usage
    // except statistics, we only update statistics if the key
    // is not deleted (otherwise it's ot used), we don't need to
    // take care about inserting, we can directly call an updating
    // to keep the same workflow like it was added in live
    if(root->mode == ZDB_MODE_SEQUENTIAL)
        return index_update_memory_handler_sequential(root, &setter, NULL);

    // others mode (aka userkey mode)
    if((existing = index_get(root, id, entry->idlength)))
        return index_update_memory_handler_memkey(root, &setter, existing);

    return index_insert_memory_handler_memkey(root, &setter);
}
