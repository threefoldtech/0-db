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

int command_exists(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        printf("[-] invalid key size\n");
        redis_hardsend(client->fd, "-Invalid key");
        return 1;
    }

    debug("[+] command: exists: lookup key: ");
    debughex(request->argv[1]->buffer, request->argv[1]->length);
    debug("\n");

    index_entry_t *entry = redis_get_handlers[rootsettings.mode](client);

    debug("[+] command: exists: entry found: %s\n", (entry ? "yes" : "no"));
    int found = (entry == NULL) ? 0 : 1;

    char response[32];
    sprintf(response, ":%d\r\n", found);

    send(client->fd, response, strlen(response), 0);

    return 0;
}

int command_check(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        printf("[-] invalid key size\n");
        redis_hardsend(client->fd, "-Invalid key");
        return 1;
    }

    debug("[+] command: check: lookup key: ");
    debughex(request->argv[1]->buffer, request->argv[1]->length);
    debug("\n");

    index_entry_t *entry = redis_get_handlers[rootsettings.mode](client);

    // key not found at all
    if(!entry) {
        verbose("[-] command: check: key not found\n");
        redis_hardsend(client->fd, "$-1");
        return 1;
    }

    // key found but deleted
    if(entry->flags & INDEX_ENTRY_DELETED) {
        verbose("[-] command: check: key deleted\n");
        redis_hardsend(client->fd, "$-1");
        return 1;
    }

    // key found and valid, let's checking the contents
    debug("[+] command: get: entry found, flags: %x, data length: %" PRIu64 "\n", entry->flags, entry->length);
    debug("[+] command: get: data file: %d, data offset: %" PRIu64 "\n", entry->dataid, entry->offset);

    data_root_t *data = client->ns->data;
    int status = data_check(data, entry->offset, entry->dataid);

    char response[32];
    sprintf(response, ":%d\r\n", status);

    send(client->fd, response, strlen(response), 0);

    return 0;
}

int command_del(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > MAX_KEY_LENGTH) {
        printf("[-] command: del: invalid key size\n");
        redis_hardsend(client->fd, "-Invalid key");
        return 1;
    }

    if(!client->writable) {
        debug("[-] command: set: denied, read-only namespace\n");
        redis_hardsend(client->fd, "-Namespace is in read-only mode");
        return 1;
    }

    index_root_t *index = client->ns->index;
    data_root_t *data = client->ns->data;
    index_entry_t *entry;

    // grabbing original entry
    if(!(entry = redis_get_handlers[rootsettings.mode](client))) {
        debug("[-] command: del: key not found\n");
        redis_hardsend(client->fd, "-Key not found");
        return 1;
    }

    // avoid double deletion
    if(index_entry_is_deleted(entry)) {
        debug("[-] command: del: key already deleted\n");
        redis_hardsend(client->fd, "-Key not found");
        return 1;
    }

    // add a new entry containing new flag
    if(!index_entry_delete(index, entry)) {
        debug("[-] command: del: index delete flag failed\n");
        redis_hardsend(client->fd, "-Cannot delete key");
        return 0;
    }

    // deleting data part
    if(!data_delete(data, entry->offset, entry->dataid)) {
        debug("[-] command: del: deleting data failed\n");
        redis_hardsend(client->fd, "-Cannot delete key");
        return 0;
    }

    redis_hardsend(client->fd, "+OK");

    return 0;
}

