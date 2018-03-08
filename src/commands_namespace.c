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

// create a new namespace
//   NSNEW [namespace]
int command_nsnew(redis_client_t *client) {
    resp_request_t *request = client->request;
    char target[COMMAND_MAXLEN];

    if(!command_admin_authorized(client))
        return 1;

    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > 128) {
        redis_hardsend(client->fd, "-Namespace too long");
        return 1;
    }

    // get string formatted namespace
    sprintf(target, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);

    // deny already existing namespace
    if(namespace_get(target)) {
        debug("[-] command: mkns: namespace already exists\n");
        redis_hardsend(client->fd, "-This namespace is not available");
        return 1;
    }

    // creating the new namespace
    // note: password needs to be set via NSSET after creation
    if(!namespace_create(target)) {
        redis_hardsend(client->fd, "-Could not create namespace");
        return 1;
    }

    redis_hardsend(client->fd, "+OK");

    return 0;
}

// change user active namespace
//   SELECT [namespace]
int command_select(redis_client_t *client) {
    resp_request_t *request = client->request;
    char target[COMMAND_MAXLEN];
    namespace_t *namespace = NULL;

    if(request->argc < 2) {
        redis_hardsend(client->fd, "-Invalid argument");
        return 1;
    }

    if(request->argv[1]->length > 128) {
        redis_hardsend(client->fd, "-Namespace too long");
        return 1;
    }

    // get name as usable string
    sprintf(target, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);

    // checking for existing namespace
    if(!(namespace = namespace_get(target))) {
        debug("[-] command: select: namespace not found\n");
        redis_hardsend(client->fd, "-Namespace not found");
        return 1;
    }

    // by default, we arrume the namespace will be read-write allowed
    int writable = 1;

    // checking if password is set
    if(namespace->password) {
        if(request->argc < 3) {
            // no password provided
            // if the namespace allows public access, we authorize it
            // but in read-only mode
            if(!namespace->public) {
                debug("[-] command: select: namespace not public and password not provided\n");
                redis_hardsend(client->fd, "-Namespace protected and private");
                return 1;
            }

            // namespace is public, which means we are
            // in a read-only mode now
            debug("[-] command: select: protected namespace, no password set, setting read-only\n");
            writable = 0;

        } else {
            char password[256];

            if(request->argv[2]->length > (ssize_t) sizeof(password) - 1) {
                redis_hardsend(client->fd, "-Password too long");
                return 1;
            }

            // copy password to a temporary variable
            // to check password match using strcmp and no any strncmp
            // to ensure we check exact password
            sprintf(password, "%.*s", request->argv[2]->length, (char *) request->argv[2]->buffer);

            if(strcmp(password, namespace->password) != 0) {
                redis_hardsend(client->fd, "-Access denied");
                return 1;
            }

            debug("[-] command: select: protected and password match, access granted\n");

            // password provided and match
        }

        // access granted
    }

    // switching client's active namespace
    debug("[+] command: select: moving user to namespace '%s'\n", namespace->name);
    client->ns = namespace;
    client->writable = writable;

    // return confirmation
    redis_hardsend(client->fd, "+OK");

    return 0;
}

// list available namespaces (all of them)
//   NSLIST (no arguments)
int command_nslist(redis_client_t *client) {
    char line[512];
    ns_root_t *nsroot = namespace_get_list();

    // streaming list to the client
    sprintf(line, "*%lu\r\n", nsroot->length);
    send(client->fd, line, strlen(line), 0);

    debug("[+] command: nslist: sending %lu items\n", nsroot->length);

    // sending each namespace line by line
    for(size_t i = 0; i < nsroot->length; i++) {
        namespace_t *ns = nsroot->namespaces[i];

        sprintf(line, "$%ld\r\n%s\r\n", strlen(ns->name), ns->name);
        send(client->fd, line, strlen(line), 0);
    }

    return 0;
}

// get information about a namespace
//   NSINFO [namespace]
int command_nsinfo(redis_client_t *client) {
    resp_request_t *request = client->request;
    char info[1024];
    char target[COMMAND_MAXLEN];
    namespace_t *namespace;

    if(!command_args_validate(client, 2))
        return 1;

    if(request->argv[1]->length > 128) {
        redis_hardsend(client->fd, "-Namespace too long");
        return 1;
    }

    // get name as usable string
    sprintf(target, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);

    // checking for existing namespace
    if(!(namespace = namespace_get(target))) {
        debug("[-] command: nsinfo: namespace not found\n");
        redis_hardsend(client->fd, "-Namespace not found");
        return 1;
    }

    sprintf(info, "# namespace\n");
    sprintf(info + strlen(info), "name: %s\n", namespace->name);
    sprintf(info + strlen(info), "entries: %lu\n", namespace->index->entries);
    sprintf(info + strlen(info), "public: %s\n", namespace->public ? "yes" : "no");
    sprintf(info + strlen(info), "password: %s\n", namespace->password ? "yes" : "no");
    sprintf(info + strlen(info), "data_size_bytes: %lu\n", namespace->index->datasize);
    sprintf(info + strlen(info), "data_size_mb: %.2f\n", namespace->index->datasize / (1024 * 1024.0));
    sprintf(info + strlen(info), "data_limits_bytes: %lu\n", namespace->maxsize);
    sprintf(info + strlen(info), "index_size_bytes: %lu\n", namespace->index->indexsize);
    sprintf(info + strlen(info), "index_size_kb: %.2f\n", namespace->index->indexsize / 1024.0);

    redis_bulk_t response = redis_bulk(info, strlen(info));
    if(!response.buffer) {
        redis_hardsend(client->fd, "$-1");
        return 0;
    }

    send(client->fd, response.buffer, response.length, 0);

    free(response.buffer);

    return 0;
}

// change namespace settings
//   NSSET [namespace] password *        -> clear password
//   NSSET [namespace] password [foobar] -> set password to 'foobar'
//   NSSET [namespace] maxsize [123456]  -> set maximum datasize to '123456'
//                                          if this is more than actual size, there
//                                          is no shrink, it stay as it
//   NSSET [namespace] public [1 or 0]   -> enable or disable public access
int command_nsset(redis_client_t *client) {
    resp_request_t *request = client->request;
    namespace_t *namespace = NULL;
    char target[COMMAND_MAXLEN];
    char command[COMMAND_MAXLEN];
    char value[COMMAND_MAXLEN];

    if(!command_admin_authorized(client))
        return 1;

    if(!command_args_validate(client, 4))
        return 1;

    if(request->argv[1]->length > 128) {
        redis_hardsend(client->fd, "-Namespace too long");
        return 1;
    }

    if(request->argv[2]->length > COMMAND_MAXLEN || request->argv[3]->length > COMMAND_MAXLEN) {
        redis_hardsend(client->fd, "-Argument too long");
        return 1;
    }

    sprintf(target, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);
    sprintf(command, "%.*s", request->argv[2]->length, (char *) request->argv[2]->buffer);
    sprintf(value, "%.*s", request->argv[3]->length, (char *) request->argv[3]->buffer);

    // limit size of the value
    if(request->argv[3]->length > 63) {
        redis_hardsend(client->fd, "-Invalid value");
        return 1;
    }

    // default namespace cannot be changed
    if(strcmp(target, NAMESPACE_DEFAULT) == 0) {
        redis_hardsend(client->fd, "-Cannot update default namespace");
        return 1;
    }

    // checking for existing namespace
    if(!(namespace = namespace_get(target))) {
        debug("[-] command: nsset: namespace not found\n");
        redis_hardsend(client->fd, "-Namespace not found");
        return 1;
    }

    //
    // testing properties
    //
    if(strcmp(command, "maxsize") == 0) {
        namespace->maxsize = atoll(value);
        debug("[+] command: nsset: new size limit: %lu\n", namespace->maxsize);

    } else if(strcmp(command, "password") == 0) {
        // clearing password using "*" password
        if(strcmp(value, "*") == 0) {
            free(namespace->password);
            namespace->password = NULL;

            debug("[+] command: nsset: password cleared\n");

            // updating password
        } else {
            namespace->password = strdup(value);
            debug("[+] command: nsset: password set and updated\n");
        }


    } else if(strcmp(command, "public") == 0) {
        namespace->public = (value[0] == '1') ? 1 : 0;
        debug("[+] command: nsset: changing public view to: %d\n", namespace->public);

    } else {
        debug("[-] command: nsset: unknown property '%s'\n", command);
        redis_hardsend(client->fd, "-Invalid property");
        return 1;
    }

    // update persistant setting
    namespace_commit(namespace);

    // confirmation
    redis_hardsend(client->fd, "+OK");

    return 0;
}

int command_dbsize(redis_client_t *client) {
    char response[64];

    sprintf(response, ":%lu\r\n", client->ns->index->entries);
    send(client->fd, response, strlen(response), 0);

    return 0;
}

