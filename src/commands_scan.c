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
#include "data.h"
#include "namespace.h"
#include "redis.h"
#include "commands.h"
#include "commands_get.h"

static int command_scan_send_array(data_entry_header_t *header, redis_client_t *client) {
    char response[(MAX_KEY_LENGTH * 2) + 128];
    size_t offset = 0;

    // array response, with 2 arguments:
    //  - first one is the next SCAN key value
    //    (in our case, this is always the same value as the returned id)
    //  - the second one is another array, containins list of keys scanned
    //    (in our case, always only one single response)
    offset = sprintf(response, "*2\r\n$%d\r\n", header->idlength);

    // copy the key
    memcpy(response + offset, header->id, header->idlength);
    offset += header->idlength;

    // end of the key
    // adding the array of response with the single response
    offset += sprintf(response + offset, "\r\n*1\r\n$%d\r\n", header->idlength);

    // writing (again) the key, but this time as id response
    memcpy(response + offset, header->id, header->idlength);
    offset += header->idlength;
    offset += sprintf(response + offset, "\r\n");

    send(client->fd, response, offset, 0);

    return 0;
}

//
// SCAN
//
int command_scan(redis_client_t *client) {
    index_entry_t *entry = NULL;
    data_scan_t scan;

    if(!command_args_validate(client, 2))
        return 1;

    // grabbing original entry
    if(!(entry = redis_get_handlers[rootsettings.mode](client))) {
        debug("[-] command: scan: key not found\n");
        redis_hardsend(client->fd, "-Invalid index");
        return 1;
    }

    if(index_entry_is_deleted(entry)) {
        verbose("[-] command: scan: key deleted\n");
        redis_hardsend(client->fd, "-Invalid index");
        return 1;
    }

    scan = data_next_header(client->ns->data, entry->dataid, entry->offset);

    if(scan.status == DATA_SCAN_SUCCESS) {
        command_scan_send_array(scan.header, client);
        free(scan.header);
    }

    if(scan.status == DATA_SCAN_UNEXPECTED) {
        redis_hardsend(client->fd, "-Internal Error");
        return 1;
    }

    if(scan.status == DATA_SCAN_NO_MORE_DATA) {
        redis_hardsend(client->fd, "-No more data");
        return 1;
    }

    return 0;
}

//
// RSCAN
//
int command_rscan(redis_client_t *client) {
    index_entry_t *entry = NULL;
    data_scan_t scan;

    if(!command_args_validate(client, 2))
        return 1;

    // grabbing original entry
    if(!(entry = redis_get_handlers[rootsettings.mode](client))) {
        debug("[-] command: scan: key not found\n");
        redis_hardsend(client->fd, "-Invalid index");
        return 1;
    }

    if(index_entry_is_deleted(entry)) {
        verbose("[-] command: scan: key deleted\n");
        redis_hardsend(client->fd, "-Invalid index");
        return 1;
    }

    scan = data_previous_header(client->ns->data, entry->dataid, entry->offset);

    if(scan.status == DATA_SCAN_SUCCESS) {
        command_scan_send_array(scan.header, client);
        free(scan.header);
    }

    if(scan.status == DATA_SCAN_UNEXPECTED) {
        redis_hardsend(client->fd, "-Internal Error");
        return 1;
    }

    if(scan.status == DATA_SCAN_NO_MORE_DATA) {
        redis_hardsend(client->fd, "-No more data");
        return 1;
    }

    return 0;
}


