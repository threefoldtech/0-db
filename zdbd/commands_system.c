#define _GNU_SOURCE
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
#include "zdbd.h"
#include "redis.h"
#include "commands.h"

int command_ping(redis_client_t *client) {
    redis_hardsend(client, "+PONG");
    return 0;
}

int command_time(redis_client_t *client) {
    struct timeval now;
    char response[256];
    char sec[64], usec[64];

    gettimeofday(&now, NULL);

    // by default the redis protocol returns values as string
    // not as an integer
    sprintf(sec, "%lu", now.tv_sec);
    sprintf(usec, "%ld", (long) now.tv_usec);

    // *2             \r\n  -- array of two values
    // $[sec-length]  \r\n  -- first header, string value
    // [seconds]      \r\n  -- first value payload
    // $[usec-length] \r\n  -- second header, string value
    // [usec]         \r\n  -- second value payload
    sprintf(response, "*2\r\n$%lu\r\n%s\r\n$%lu\r\n%s\r\n", strlen(sec), sec, strlen(usec), usec);

    redis_reply_stack(client, response, strlen(response));

    return 0;
}

// STOP will be only compiled in debug mode
// this will force to exit the listen loop in order to call
// all destructors, which is useful to ensure every memory allocations
// are well tracked and well cleaned up
//
// in production, a user should not be able to stop the daemon
int command_stop(redis_client_t *client) {
    #ifndef RELEASE
        redis_hardsend(client, "+Stopping");
        return RESP_STATUS_SHUTDOWN;
    #else
        redis_hardsend(client, "-Unauthorized");
        return 0;
    #endif
}

int command_info(redis_client_t *client) {
    char info[4096];
    struct timeval current;
    zdb_settings_t *zdb_settings = zdb_settings_get();
    zdb_stats_t *lstats = &zdb_settings->stats;
    zdbd_stats_t *dstats = &zdbd_rootsettings.stats;
    gettimeofday(&current, NULL);

    sprintf(info, "# server\n");
    sprintf(info + strlen(info), "server_name: 0-db (zdb)\n");
    sprintf(info + strlen(info), "server_revision: " ZDBD_REVISION "\n");
    sprintf(info + strlen(info), "engine_revision: %s\n", zdb_revision());
    sprintf(info + strlen(info), "instance_id: %u\n", zdb_instanceid_get());
    sprintf(info + strlen(info), "boot_time: %ld\n", dstats->boottime.tv_sec);
    sprintf(info + strlen(info), "uptime: %ld\n", current.tv_sec - dstats->boottime.tv_sec);


    sprintf(info + strlen(info), "\n# clients\n");
    sprintf(info + strlen(info), "clients_lifetime: %" PRIu32 "\n", dstats->clients);


    sprintf(info + strlen(info), "\n# stats\n");
    sprintf(info + strlen(info), "commands_executed: %" PRIu64 "\n", dstats->cmdsvalid);
    sprintf(info + strlen(info), "commands_failed: %" PRIu64 "\n", dstats->cmdsfailed);
    sprintf(info + strlen(info), "commands_unauthorized: %" PRIu64 "\n", dstats->adminfailed);

    sprintf(info + strlen(info), "index_disk_read_failed: %" PRIu64 "\n", lstats->idxreadfailed);
    sprintf(info + strlen(info), "index_disk_write_failed: %" PRIu64 "\n", lstats->idxwritefailed);
    sprintf(info + strlen(info), "data_disk_read_failed: %" PRIu64 "\n", lstats->datareadfailed);
    sprintf(info + strlen(info), "data_disk_write_failed: %" PRIu64 "\n", lstats->datawritefailed);

    sprintf(info + strlen(info), "index_disk_read_bytes: %" PRIu64 "\n", lstats->idxdiskread);
    sprintf(info + strlen(info), "index_disk_read_mb: %.2f\n", lstats->idxdiskread / (1024 * 1024.0));
    sprintf(info + strlen(info), "index_disk_write_bytes: %" PRIu64 "\n", lstats->idxdiskwrite);
    sprintf(info + strlen(info), "index_disk_write_mb: %.2f\n", lstats->idxdiskwrite / (1024 * 1024.0));

    sprintf(info + strlen(info), "data_disk_read_bytes: %" PRIu64 "\n", lstats->datadiskread);
    sprintf(info + strlen(info), "data_disk_read_mb: %.2f\n", lstats->datadiskread / (1024 * 1024.0));
    sprintf(info + strlen(info), "data_disk_write_bytes: %" PRIu64 "\n", lstats->datadiskwrite);
    sprintf(info + strlen(info), "data_disk_write_mb: %.2f\n", lstats->datadiskwrite / (1024 * 1024.0));

    sprintf(info + strlen(info), "network_rx_bytes: %" PRIu64 "\n", dstats->networkrx);
    sprintf(info + strlen(info), "network_rx_mb: %.2f\n", dstats->networkrx / (1024 * 1024.0));
    sprintf(info + strlen(info), "network_tx_bytes: %" PRIu64 "\n", dstats->networktx);
    sprintf(info + strlen(info), "network_tx_mb: %.2f\n", dstats->networktx / (1024 * 1024.0));

    redis_bulk_t response = redis_bulk(info, strlen(info));
    if(!response.buffer) {
        redis_hardsend(client, "$-1");
        return 0;
    }

    redis_reply_stack(client, response.buffer, response.length);
    free(response.buffer);

    return 0;
}

int command_hooks(redis_client_t *client) {
    zdb_settings_t *zdb_settings = zdb_settings_get();
    zdb_hooks_t *hooks = &zdb_settings->hooks;
    hook_t *hook = NULL;
    char *info;

    if(!command_admin_authorized(client))
        return 1;

    if(!(info = calloc(sizeof(char), 8192)))
        return 1;

    sprintf(info, "*%lu\r\n", hooks->active);

    for(size_t i = 0; i < hooks->length; i++) {
        if(!(hook = hooks->hooks[i]))
            continue;

        char *type = hook->argv[1];

        sprintf(info + strlen(info), "*6\r\n");

        // hook type
        sprintf(info + strlen(info), "$%lu\r\n%s\r\n", strlen(type), type);

        // hook arguments
        sprintf(info + strlen(info), "*%lu\r\n", hook->argc - 4);

        // skipping:
        //  - 1st argument: hook binary
        //  - 2nd argument: which is the type
        //  - 3rd argument: which is the instance id
        //  - last argument: which is always null
        for(size_t s = 3; s < hook->argc - 1; s++) {
            char *arg = hook->argv[s];
            sprintf(info + strlen(info), "$%lu\r\n%s\r\n", strlen(arg), arg);
        }

        // hook pid
        sprintf(info + strlen(info), ":%d\r\n", hook->pid);

        // timestamp when started
        sprintf(info + strlen(info), ":%ld\r\n", hook->created);

        // timestamp when finished
        sprintf(info + strlen(info), ":%ld\r\n", hook->finished);

        // exit code value
        sprintf(info + strlen(info), ":%d\r\n", hook->status);
    }

    redis_reply_heap(client, info, strlen(info), free);

    return 0;
}

static int command_index_dirty(redis_client_t *client) {
    resp_request_t *request = client->request;
    index_root_t *index = client->ns->index;
    char subcommand[COMMAND_MAXLEN];
    char *response;

    if(request->argc == 3) {
        if(!command_args_overflow(client, 2, COMMAND_MAXLEN))
            return 1;

        sprintf(subcommand, "%.*s", request->argv[2]->length, (char *) request->argv[2]->buffer);

        if(strcasecmp(subcommand, "RESET") == 0) {
            index_dirty_reset(index);
            redis_hardsend(client, "+OK");
            return 0;
        }

        redis_hardsend(client, "-Unknown INDEX DIRTY subcommand");
        return 1;
    }

    if(!(response = calloc(sizeof(char), 2048))) {
        zdbd_warnp("index: dirty: calloc");
        redis_hardsend(client, "-Internal Memory Error");
        return 1;
    }

    size_t compute = 0;

    // FIXME: avoid double loop
    for(size_t i = 0; i < index->dirty.length; i++)
        for(int a = 0; a < 8; a++)
            if(index_dirty_get(index, (i * 8) + a) == 1)
                compute += 1;

    sprintf(response, "*%lu\r\n", compute);

    // listing dirty index files
    for(size_t i = 0; i < index->dirty.length; i++) {
        for(int a = 0; a < 8; a++) {
            int fileid = (i * 8) + a;

            if(index_dirty_get(index, fileid) == 1)
                sprintf(response + strlen(response), ":%d\r\n", fileid);
        }
    }

    redis_reply_heap(client, response, strlen(response), free);

    return 0;
}

int command_index(redis_client_t *client) {
    resp_request_t *request = client->request;
    char command[COMMAND_MAXLEN];

    if(!command_admin_authorized(client))
        return 1;

    if(!command_args_validate_min(client, 2))
        return 1;

    if(!command_args_overflow(client, 1, COMMAND_MAXLEN))
        return 1;

    sprintf(command, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);

    if(strcasecmp(command, "DIRTY") == 0)
        return command_index_dirty(client);

    redis_hardsend(client, "-Unknown INDEX subcommand");
    return 1;
}

