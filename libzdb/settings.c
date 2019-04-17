#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include "libzdb.h"
#include "libzdb_private.h"

static char *zdb_modes[] = {
    "default key-value",
    "sequential keys",
    "direct key position",
    "direct key fixed block length",
};

zdb_settings_t *zdb_settings_get() {
    return &zdb_rootsettings;
}

char *zdb_running_mode(index_mode_t mode) {
    if(mode > (sizeof(zdb_modes) / sizeof(char *)))
        return "unsupported mode";

    return zdb_modes[mode];
}

char *zdb_id() {
    if(!zdb_rootsettings.zdbid)
        return "unknown-id";

    return zdb_rootsettings.zdbid;
}

char *zdb_id_set(char *listenaddr, int port, char *socket) {
    if(socket) {
        // unix socket
        if(asprintf(&zdb_rootsettings.zdbid, "unix://%s", socket) < 0)
            zdb_diep("asprintf");

        return zdb_rootsettings.zdbid;
    }

    // default tcp
    if(asprintf(&zdb_rootsettings.zdbid, "tcp://%s:%d", listenaddr, port) < 0)
        zdb_diep("asprintf");

    return zdb_rootsettings.zdbid;
}

uint32_t zdb_instanceid_generate() {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    srand((time_t) ts.tv_nsec);

    // generating random id, greater than zero
    zdb_rootsettings.iid = (uint32_t) ((rand() % (1 << 30)) + 1);

    return zdb_rootsettings.iid;
}

uint32_t zdb_instanceid_get() {
    return zdb_rootsettings.iid;
}

char *zdb_version() {
    return ZDB_VERSION;
}

char *zdb_revision() {
    return ZDB_REVISION;
}
