#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <inttypes.h>
#include "libzdb.h"
#include "zdbd.h"
#include "redis.h"
#include "commands.h"
#include "commands_scan.h"

uint64_t ustime() {
    struct timeval tv;
    uint64_t ust;

    gettimeofday(&tv, NULL);

    ust = ((uint64_t) tv.tv_sec) * 1000000;
    ust += tv.tv_usec;

    return ust;
}

//
// scan list management
//

// append one entry into the scanlist result
// if the list is not large enough to contains the entry
// growing it up a little bit
static scan_list_t *scanlist_append(scan_list_t *scanlist, index_scan_t *scan, scan_info_t *info) {
    if(scanlist->length + 1 > scanlist->allocated) {
        scanlist->allocated += 32;

        if(!(scanlist->items = realloc(scanlist->items, scanlist->allocated * sizeof(index_item_t *))))
            return NULL;

        if(!(scanlist->scansinfo = realloc(scanlist->scansinfo, scanlist->allocated * sizeof(scan_info_t))))
            return NULL;
    }

    scanlist->items[scanlist->length] = scan->header;
    scanlist->scansinfo[scanlist->length] = *info;
    scanlist->length += 1;

    return scanlist;
}

static void scanlist_init(scan_list_t *scanlist) {
    memset(scanlist, 0x00, sizeof(scan_list_t));
}

static void scanlist_free(scan_list_t *scanlist) {
    for(size_t i = 0; i < scanlist->length; i++)
        free(scanlist->items[i]);

    free(scanlist->items);
    free(scanlist->scansinfo);
}

static void scaninfo_dump(scan_info_t *info) {
#ifdef RELEASE
    (void) info;
#else
    zdbd_debug("[+] scaninfo: idx: %u, offset: %lu, data: %u\n", info->idxid, info->idxoffset, info->dataid);
#endif
}

static void scaninfo_from_scan(scan_info_t *info, index_scan_t *scan) {
    info->dataid = scan->header->dataid;
    info->idxid = scan->fileid;
    info->idxoffset = scan->target;

    scaninfo_dump(info);
}

static void scaninfo_from_entry(scan_info_t *info, index_entry_t *entry) {
    info->dataid = entry->dataid;
    info->idxid = entry->indexid;
    info->idxoffset = entry->idxoffset;

    scaninfo_dump(info);
}

//
// redis serialization of the scan list
//
static int command_scan_send_scanlist(scan_list_t *scanlist, redis_client_t *client) {
    char *response;
    size_t offset = 0;
    index_item_t *entry;
    scan_info_t *scaninfo;
    index_bkey_t bkey;

    // if the list is empty, we have nothing
    // to send, obviously
    if(scanlist->length == 0) {
        redis_hardsend(client, "-No more data");
        return 0;
    }

    for(size_t i = 0; i < scanlist->length; i++) {
        scaninfo_dump(&scanlist->scansinfo[i]);
    }

    // array response, with 2 arguments:
    //  - first one is the next SCAN key value
    //    (in our case, this is always the same value as the returned id)
    //  - the second one is another array, of each keys found, each entry containins
    //    information about this key like timestamp and size
    if(!(response = malloc(((MAX_KEY_LENGTH * 2) + 128) * scanlist->length)))
        return 1;

    // converting the last object key into a binary serialized key
    entry = scanlist->items[scanlist->length - 1];
    scaninfo = &scanlist->scansinfo[scanlist->length - 1];
    bkey = index_item_serialize(entry, scaninfo->idxoffset, scaninfo->idxid);

    // get last entry for the next key value
    offset = sprintf(response, "*2\r\n$%ld\r\n", sizeof(index_bkey_t));

    // copy the key
    memcpy(response + offset, &bkey, sizeof(index_bkey_t));
    offset += sizeof(index_bkey_t);

    // iterating over the full list and building the list response
    offset += sprintf(response + offset, "\r\n*%lu\r\n", scanlist->length);

    for(size_t i = 0; i < scanlist->length; i++) {
        entry = scanlist->items[i];

        // end of the key
        // adding the array of response with the single response
        offset += sprintf(response + offset, "*3\r\n");

        // adding the key
        offset += sprintf(response + offset, "$%u\r\n", entry->idlength);
        memcpy(response + offset, entry->id, entry->idlength);
        offset += entry->idlength;

        // adding the length of the payload
        offset += sprintf(response + offset, "\r\n:%d\r\n", entry->length);

        // adding the timestamp to the payload
        offset += sprintf(response + offset, ":%d\r\n", entry->timestamp);
    }

    redis_reply_heap(client, response, offset, free);

    return 0;
}

static scan_info_t *scan_initial_info(scan_info_t *info, scan_list_t *scanlist, index_scan_t *scan) {
    // could not get initial scan entry
    if(scan->status != INDEX_SCAN_SUCCESS)
        return NULL;

    scaninfo_from_scan(info, scan);
    scanlist_append(scanlist, scan, info);

    return info;
}

static scan_info_t *scan_initial_get(scan_info_t *info, redis_client_t *client) {
    index_entry_t *entry = NULL;
    index_bkey_t bkey;

    if(client->request->argv[1]->length != sizeof(index_bkey_t)) {
        zdbd_debug("[-] command: scan: requested key invalid (size mismatch)\n");
        redis_hardsend(client, "-Invalid key format");
        return NULL;
    }

    memcpy(&bkey, client->request->argv[1]->buffer, sizeof(index_bkey_t));

    if(!(entry = index_entry_deserialize(client->ns->index, &bkey))) {
        zdbd_debug("[-] command: scan: could not fetch/validate key requested\n");
        redis_hardsend(client, "-Invalid key format");
        return NULL;
    }

    // FIXME
    #if 0
    // grabbing original entry
    if(!(entry = redis_get_handlers[rootsettings.mode](client))) {
        zdbd_debug("[-] command: scan: key not found\n");
        redis_hardsend(client, "-Invalid index");
        return NULL;
    }

    if(index_entry_is_deleted(entry)) {
        zdbd_verbose("[-] command: scan: key deleted\n");
        redis_hardsend(client, "-Invalid index");
        return NULL;
    }
    #endif

    scaninfo_from_entry(info, entry);
    // scaninfo_dump(info);
    free(entry);

    return info;
}

static int scan_failure(index_scan_t *scan, redis_client_t *client) {
    if(scan->status == INDEX_SCAN_NO_MORE_DATA)
        redis_hardsend(client, "-No more data");

    if(scan->status == INDEX_SCAN_UNEXPECTED)
        redis_hardsend(client, "-Internal Error");

    return 1;
}

//
// SCAN
//
int command_scan(redis_client_t *client) {
    index_scan_t scan;
    scan_list_t scanlist;
    scan_info_t info;

    if(namespace_is_frozen(client->ns))
        return command_error_frozen(client);

    // initialize empty scanlist
    scanlist_init(&scanlist);

    // scan requested without initial key
    if(client->request->argc == 1) {
        scan = index_first_header(client->ns->index);

        if(!scan_initial_info(&info, &scanlist, &scan))
            return scan_failure(&scan, client);

    } else {
        // scan requested with an initial key
        if(!scan_initial_get(&info, client))
            return 1;
    }

    // we have everything needed to start walking over
    // the keys and building our scan response
    uint64_t basetime = ustime();

    while(ustime() - basetime < SCAN_TIMESLICE_US) {
        zdbd_debug("[+] scan: elapsed time: %" PRIu64 " us\n", ustime() - basetime);

        // reading entry and appending it
        scan = index_next_header(client->ns->index, info.idxid, info.idxoffset);

        // this scan failed, let's guess it's the end
        if(scan.status != INDEX_SCAN_SUCCESS)
            break;

        // preparing next call
        scaninfo_from_scan(&info, &scan);

        // append object to the list
        scanlist_append(&scanlist, &scan, &info);
    }

    zdbd_debug("[+] scan: retreived %lu entries in %" PRIu64 " us\n", scanlist.length, ustime() - basetime);

    if(command_scan_send_scanlist(&scanlist, client))
        redis_hardsend(client, "-Internal Error");

    scanlist_free(&scanlist);
    return 0;
}

//
// RSCAN
//
int command_rscan(redis_client_t *client) {
    index_scan_t scan;
    scan_list_t scanlist;
    scan_info_t info;

    if(namespace_is_frozen(client->ns))
        return command_error_frozen(client);

    // initialize empty scanlist
    scanlist_init(&scanlist);

    // scan requested without initial key
    if(client->request->argc == 1) {
        scan = index_last_header(client->ns->index);

        if(!scan_initial_info(&info, &scanlist, &scan))
            return scan_failure(&scan, client);

    } else {
        // scan requested with an initial key
        if(!scan_initial_get(&info, client))
            return 1;
    }

    // we have everything needed to start walking over
    // the keys and building our scan response
    uint64_t basetime = ustime();

    while(ustime() - basetime < SCAN_TIMESLICE_US) {
        zdbd_debug("[+] rscan: elapsed time: %" PRIu64 " us\n", ustime() - basetime);

        // reading entry and appending it
        scan = index_previous_header(client->ns->index, info.idxid, info.idxoffset);

        // this scan failed, let's guess it's the end
        if(scan.status != INDEX_SCAN_SUCCESS)
            break;

        // preparing next call
        scaninfo_from_scan(&info, &scan);

        // append object to the list
        scanlist_append(&scanlist, &scan, &info);
    }

    zdbd_debug("[+] rscan: retreived %lu entries in %" PRIu64 " us\n", scanlist.length, ustime() - basetime);

    if(command_scan_send_scanlist(&scanlist, client))
        redis_hardsend(client, "-Internal Error");

    scanlist_free(&scanlist);
    return 0;
}

//
// KEYCUR
//
int command_keycur(redis_client_t *client) {
    resp_request_t *request = client->request;
    index_entry_t *entry;

    if(!command_args_validate(client, 2))
        return 1;

    if(namespace_is_frozen(client->ns))
        return command_error_frozen(client);

    index_root_t *index = client->ns->index;
    resp_object_t *key = request->argv[1];

    if(!(entry = index_get(index, key->buffer, key->length))) {
        redis_hardsend(client, "-Key not found");
        return 1;
    }

    char response[64];
    size_t offset = 0;

    // building binary key
    index_bkey_t bkey = index_entry_serialize(entry);
    index_entry_dump(entry);

    // building binary response string
    offset += snprintf(response, sizeof(response), "$%lu\r\n", sizeof(index_bkey_t));

    memcpy(response + offset, &bkey, sizeof(index_bkey_t));
    offset += sizeof(index_bkey_t);

    memcpy(response + offset, "\r\n", 2);
    offset += 2;

    redis_reply_stack(client, response, offset);

    return 0;
}



list_t *list_append(list_t *list, void *item) {
    if(list->length + 1 > list->allocated) {
        list->allocated += 32;

        if(!(list->items = realloc(list->items, list->allocated * sizeof(void *))))
            return NULL;
    }

    list->items[list->length] = item;
    list->length += 1;

    return list;
}

list_t list_init(list_t *list) {
    list_t empty;

    if(!list)
        list = &empty;

    memset(list, 0x00, sizeof(list_t));
    return *list;
}

void list_free(list_t *list) {
    free(list->items);
}



//
// KSCAN
//
static int command_kscan_send_list(redis_client_t *client, list_t *list) {
    char *response;
    size_t offset = 0;
    index_entry_t *entry;

    // if the list is empty, we have nothing
    // to send, obviously
    if(list->length == 0) {
        redis_hardsend(client, "-No keys match");
        return 0;
    }

    // array response, with 2 arguments:
    //  - first one is the next SCAN key value
    //    (in our case, this is always the same value as the returned id)
    //  - the second one is another array, of each keys found, each entry containins
    //    information about this key like timestamp and size
    if(!(response = malloc(((MAX_KEY_LENGTH * 2) + 128) * list->length)))
        return 1;

    offset = sprintf(response, "*2\r\n$1\r\n0\r\n");

    // iterating over the full list and building the list response
    offset += sprintf(response + offset, "*%lu\r\n", list->length);

    for(size_t i = 0; i < list->length; i++) {
        entry = list->items[i];

        // adding the key
        offset += sprintf(response + offset, "$%u\r\n", entry->idlength);
        memcpy(response + offset, entry->id, entry->idlength);
        offset += entry->idlength;


        offset += sprintf(response + offset, "\r\n");
    }

    redis_reply_heap(client, response, offset, free);

    return 0;
}

struct kscan_ptr {
    list_t *keys;
    resp_object_t *key;
};

// callback which build the list
static int command_kscan_callback(index_entry_t *entry, void *ptr) {
    struct kscan_ptr *kscan = (struct kscan_ptr *) ptr;

    if(entry->idlength < kscan->key->length)
        return 0;

    if(memcmp(entry->id, kscan->key->buffer, kscan->key->length) == 0)
        list_append(kscan->keys, entry);

    return 0;
}

int command_kscan(redis_client_t *client) {
    resp_request_t *request = client->request;
    index_root_t *index = client->ns->index;

    #ifdef RELEASE
    redis_hardsend(client, "-Command disabled in release code, not yet available");
    return 1;
    #endif

    // it doesn't make sens to do that on sequential index
    if(index->mode != ZDB_MODE_KEY_VALUE) {
        redis_hardsend(client, "-Index running mode doesn't support this feature");
        return 1;
    }

    if(!command_args_validate(client, 2))
        return 1;

    resp_object_t *key = request->argv[1];
    list_t keys = list_init(NULL);

    struct kscan_ptr kscan = {
        .keys = &keys,
        .key = key,
    };

    // build a list via index walk callback
    index_hash_walk(client->ns->index->hash, command_kscan_callback, &kscan);

    command_kscan_send_list(client, &keys);
    list_free(&keys);

    return 0;
}

