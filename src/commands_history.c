#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <inttypes.h>
#include "zerodb.h"
#include "index.h"
#include "index_get.h"
#include "index_scan.h"
#include "data.h"
#include "namespace.h"
#include "redis.h"
#include "commands.h"
#include "commands_get.h"

// history support
//
// history is a way to get data back in the time
// each time you update a key, the new key keep track of the location
// where was the previous index entry of that key
//
// this way allows us to walk over the index and follow a chain of history
//
// one more advantage to have always append data, until we compact and remove
// data, we still have everything, rolling back in time is quite easy
//
// there is one 'HISTORY' command which allows you explore key lifetime
// at first, you need to run HISTORY command with as single argument, the key you
// want to explore
//
// HISTORY always returns the same response (an array):
//   1) a binary key to use to follow the chain
//   2) the timestamp when this key was set
//   3) the actual payload, at that time
//
// the first field is a special binary data which can be used on HISTORY command
// as next argument, this binary data contains fileid and offset in the index of the
// previous entry, this is enough (like direct-mode) to get index back
//
// since you can craft yourself this thing, you need to always send the expected key
// as first argument, this will ensure the data read on the index seems legit
// (by compating the keys)
//
// when you reach the end of the chain, the binary received it nil

typedef struct history_response_t {
    uint32_t timestamp;
    index_ekey_t *ekey;
    data_payload_t payload;

} history_response_t;

static int history_send_array(redis_client_t *client, history_response_t *history) {
    char *response = NULL;
    char datestr[64];
    size_t offset = 0;

    // computing the full length expected
    size_t fullsize = history->payload.length + 256;
    if(!(response = malloc(fullsize))) {
        warnp("history send array: malloc");
        redis_hardsend(client, "-Internal Error");
        return 1;
    }

    // array response, with 3 arguments:
    //  - first one is the next HISTORY key to use for previous version
    //  - the second one is the date when the data was set
    //  - the third one is the payload itself
    if(history->ekey->indexid != 0 || history->ekey->offset != 0) {
        offset = sprintf(response, "*3\r\n$%lu\r\n", sizeof(index_ekey_t));

        memcpy(response + offset, history->ekey, sizeof(index_ekey_t));
        offset += sizeof(index_ekey_t);

    } else {
        // setting nul for next key
        offset = sprintf(response, "*3\r\n$-1");
    }

    // end of the key
    // adding the date
    sprintf(datestr, "%" PRIu32, history->timestamp);
    offset += sprintf(response + offset, "\r\n$%lu\r\n%s\r\n", strlen(datestr), datestr);

    // writing the payload
    offset += sprintf(response + offset, "$%lu\r\n", history->payload.length);
    memcpy(response + offset, history->payload.buffer, history->payload.length);
    offset += history->payload.length;

    // end of payload
    memcpy(response + offset, "\r\n", 2);
    offset += 2;

    redis_reply_stack(client, response, offset);
    free(response);

    return 0;
}

int history_send(redis_client_t *client, index_item_t *item, index_ekey_t *ekey) {
    data_root_t *data = client->ns->data;
    history_response_t response;

    // dump entry found
    index_item_header_dump(item);

    // get data payload for this entry
    response.payload = data_get(data, item->offset, item->length, item->dataid, item->idlength);

    if(!response.payload.buffer) {
        printf("[-] command: history: cannot read payload\n");
        redis_hardsend(client, "-Internal Error");
        free(response.payload.buffer);
        return 0;
    }

    response.timestamp = item->timestamp;
    response.ekey = ekey;

    debug("[+] command: history: we got everything needed, sending\n");
    history_send_array(client, &response);

    // cleaning allocated here
    free(response.payload.buffer);
    free(item);

    return 0;
}

//
// HISTORY
//
int command_history(redis_client_t *client) {
    index_root_t *index = client->ns->index;
    index_entry_t *entry = NULL;
    index_ekey_t ekey = {
        .indexid = 0,
        .offset = 0,
    };

    // only accept 1 or 2 extra arguments
    //
    // the first argument always needs to be the key
    // even when quering an old key, this allows to ensure
    // the data requested belong to the key and the user don't
    // ask for a crafted fake offset
    //
    // the second argument is an optional offset to even older
    // key, which is returned by another history command
    if(client->request->argc != 2 && client->request->argc != 3) {
        redis_hardsend(client, "-Invalid arguments");
        return 1;
    }

    // requesting a previous data, without any exact offset
    // this basicly request the first older entry of a specific key
    if(client->request->argc == 2) {
        // grabbing original entry
        if(!(entry = index_get(index, client->request->argv[1]->buffer, client->request->argv[1]->length))) {
            debug("[-] command: history: key not found\n");
            redis_hardsend(client, "-Key not found");
            return 1;
        }

        // checking for deletion
        if(index_entry_is_deleted(entry)) {
            debug("[-] command: history: key deleted, ignoring\n");
            redis_hardsend(client, "-Key not found");
            return 1;
        }

        // we can now find the parent
        ekey.indexid = entry->parentid;
        ekey.offset = entry->parentoff;

        index_item_t *item = index_item_get_disk(index, entry->dataid, entry->idxoffset, entry->idlength);

        return history_send(client, item, &ekey);
    }

    // user requested an older entry
    // let's fetch the requested entry on the index file
    // and check if this is a legitim request
    if(client->request->argv[2]->length != sizeof(index_ekey_t)) {
        // the requested exact index looks not correct
        debug("[-] command: history: invalid exact key size\n");
        redis_hardsend(client, "-Invalid arguments");
        return 1;
    }

    unsigned char *userkey = client->request->argv[1]->buffer;
    unsigned char *userekey = client->request->argv[2]->buffer;

    uint8_t idlength = client->request->argv[1]->length;
    index_item_t *item;

    memcpy(&ekey, userekey, sizeof(index_ekey_t));

    if(ekey.indexid == 0 && ekey.offset == 0) {
        debug("[-] command: history: exact key is null, nothing more to do\n");
        redis_hardsend(client, "-No more history");
        return 1;
    }

    if(!(item = index_item_get_disk(index, ekey.indexid, ekey.offset, idlength))) {
        debug("[-] command: history: cannot read index entry requested\n");
        redis_hardsend(client, "-Invalid arguments (index query)");
        return 1;
    }

    if(memcmp(userkey, item->id, idlength)) {
        debug("[-] command: history: key mismatch from user and index, denied\n");
        free(item);
        redis_hardsend(client, "-Invalid arguments (invalid key)");
        return 1;
    }

    ekey.indexid = item->parentid;
    ekey.offset = item->parentoff;

    // okay, everything seems to be legitim now
    return history_send(client, item, &ekey);
}


