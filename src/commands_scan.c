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

static int command_scan_send_array(data_entry_header_t *header, resp_request_t *request) {
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

    send(request->client->fd, response, offset, 0);

    return 0;
}

//
// SCAN
//
int command_scan(resp_request_t *request) {
    index_entry_t *entry = NULL;
    data_scan_t scan;

    if(!command_args_validate(request, 2))
        return 1;

    // grabbing original entry
    if(!(entry = redis_get_handlers[rootsettings.mode](request))) {
        debug("[-] command: scan: key not found\n");
        redis_hardsend(request->client->fd, "-Invalid index");
        return 1;
    }

    scan = data_next_header(request->client->ns->data, entry->dataid, entry->offset);

    if(scan.status == DATA_SCAN_SUCCESS) {
        command_scan_send_array(scan.header, request);
        free(scan.header);
    }

    if(scan.status == DATA_SCAN_UNEXPECTED) {
        redis_hardsend(request->client->fd, "-Internal Error");
        return 1;
    }

    if(scan.status == DATA_SCAN_NO_MORE_DATA) {
        redis_hardsend(request->client->fd, "-No more data");
        return 1;
    }

    return 0;
}

//
// RSCAN
//
int command_rscan(resp_request_t *request) {
    index_entry_t *entry = NULL;
    data_scan_t scan;

    if(!command_args_validate(request, 2))
        return 1;

    // grabbing original entry
    if(!(entry = redis_get_handlers[rootsettings.mode](request))) {
        debug("[-] command: scan: key not found\n");
        redis_hardsend(request->client->fd, "-Invalid index");
        return 1;
    }

    scan = data_previous_header(request->client->ns->data, entry->dataid, entry->offset);

    if(scan.status == DATA_SCAN_SUCCESS) {
        command_scan_send_array(scan.header, request);
        free(scan.header);
    }

    if(scan.status == DATA_SCAN_UNEXPECTED) {
        redis_hardsend(request->client->fd, "-Internal Error");
        return 1;
    }

    if(scan.status == DATA_SCAN_NO_MORE_DATA) {
        redis_hardsend(request->client->fd, "-No more data");
        return 1;
    }

    return 0;
}


