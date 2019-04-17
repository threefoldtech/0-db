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

    // default redis protocol returns values as string
    // not as integer
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

int command_auth(redis_client_t *client) {
    resp_request_t *request = client->request;

    if(!command_args_validate(client, 2))
        return 1;

    if(!zdbd_rootsettings.adminpwd) { // FIXME
        redis_hardsend(client, "-Authentification disabled");
        return 0;
    }

    if(request->argv[1]->length > 128) {
        redis_hardsend(client, "-Password too long");
        return 1;
    }

    char password[192];
    sprintf(password, "%.*s", request->argv[1]->length, (char *) request->argv[1]->buffer);

    if(strcmp(password, zdbd_rootsettings.adminpwd) == 0) { // FIXME
        client->admin = 1;
        redis_hardsend(client, "+OK");
        return 0;
    }

    redis_hardsend(client, "-Access denied");
    return 1;
}

// STOP will be only compiled in debug mode
// this will force to exit listen loop in order to call
// all destructors, this is useful to ensure every memory allocation
// are well tracked and well cleaned
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
    zdb_settings_t *zdb_settings = zdb_settings_get();
    zdb_stats_t *stats = &zdb_settings->stats;

    sprintf(info, "# server\n");
    sprintf(info + strlen(info), "server_name: 0-db (zdb)\n");
    sprintf(info + strlen(info), "server_revision: " REVISION "\n");
    sprintf(info + strlen(info), "instance_id: %u\n", zdb_settings->iid); // FIXME
    sprintf(info + strlen(info), "boot_time: %ld\n", stats->boottime);
    sprintf(info + strlen(info), "uptime: %ld\n", time(NULL) - stats->boottime);


    sprintf(info + strlen(info), "\n# clients\n");
    sprintf(info + strlen(info), "clients_lifetime: %" PRIu32 "\n", stats->clients);


    sprintf(info + strlen(info), "\n# stats\n");
    sprintf(info + strlen(info), "commands_executed: %" PRIu64 "\n", stats->cmdsvalid);
    sprintf(info + strlen(info), "commands_failed: %" PRIu64 "\n", stats->cmdsfailed);
    sprintf(info + strlen(info), "commands_unauthorized: %" PRIu64 "\n", stats->adminfailed);

    sprintf(info + strlen(info), "index_disk_read_failed: %" PRIu64 "\n", stats->idxreadfailed);
    sprintf(info + strlen(info), "index_disk_write_failed: %" PRIu64 "\n", stats->idxwritefailed);
    sprintf(info + strlen(info), "data_disk_read_failed: %" PRIu64 "\n", stats->datareadfailed);
    sprintf(info + strlen(info), "data_disk_write_failed: %" PRIu64 "\n", stats->datawritefailed);

    sprintf(info + strlen(info), "index_disk_read_bytes: %" PRIu64 "\n", stats->idxdiskread);
    sprintf(info + strlen(info), "index_disk_read_mb: %.2f\n", stats->idxdiskread / (1024 * 1024.0));
    sprintf(info + strlen(info), "index_disk_write_bytes: %" PRIu64 "\n", stats->idxdiskwrite);
    sprintf(info + strlen(info), "index_disk_write_mb: %.2f\n", stats->idxdiskwrite / (1024 * 1024.0));

    sprintf(info + strlen(info), "data_disk_read_bytes: %" PRIu64 "\n", stats->datadiskread);
    sprintf(info + strlen(info), "data_disk_read_mb: %.2f\n", stats->datadiskread / (1024 * 1024.0));
    sprintf(info + strlen(info), "data_disk_write_bytes: %" PRIu64 "\n", stats->datadiskwrite);
    sprintf(info + strlen(info), "data_disk_write_mb: %.2f\n", stats->datadiskwrite / (1024 * 1024.0));

    sprintf(info + strlen(info), "network_rx_bytes: %" PRIu64 "\n", stats->networkrx);
    sprintf(info + strlen(info), "network_rx_mb: %.2f\n", stats->networkrx / (1024 * 1024.0));
    sprintf(info + strlen(info), "network_tx_bytes: %" PRIu64 "\n", stats->networktx);
    sprintf(info + strlen(info), "network_tx_mb: %.2f\n", stats->networktx / (1024 * 1024.0));

    redis_bulk_t response = redis_bulk(info, strlen(info));
    if(!response.buffer) {
        redis_hardsend(client, "$-1");
        return 0;
    }

    redis_reply_stack(client, response.buffer, response.length);
    free(response.buffer);

    return 0;
}

