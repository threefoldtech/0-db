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
#include "commands_mirror.h"

int command_mirror(redis_client_t *client) {
    if(!command_admin_authorized(client))
        return 1;

    client->mirror = 1;
    redis_hardsend(client, "+Starting mirroring");

    return 0;
}

int command_master(redis_client_t *client) {
    if(!command_admin_authorized(client))
        return 1;

    client->master = 1;
    redis_hardsend(client, "+Hello, master");

    return 0;
}

