#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include "libzdb.h"
#include "libzdb_private.h"

static char *zdb_modes[] = {
    "default key-value",
    "sequential keys",
    "direct key position",
    "direct key fixed block length",
};

static uint32_t zdb_instanceid_generate() {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    srand((time_t) ts.tv_nsec);

    // generating random id, greater than zero
    return (uint32_t) ((rand() % (1 << 30)) + 1);
}

//
// main settings initializer
//
zdb_settings_t *zdb_initialize() {
    zdb_settings_t *s = &zdb_rootsettings;

    // apply default settings
    s->datapath = ZDB_DEFAULT_DATAPATH;
    s->indexpath = ZDB_DEFAULT_INDEXPATH;
    s->datasize = ZDB_DEFAULT_DATA_MAXSIZE;
    s->mode = KEYVALUE;

    // resetting values
    s->verbose = 0;
    s->dump = 0;
    s->sync = 0;
    s->synctime = 0;
    s->hook = NULL;
    s->maxsize = 0;

    // initialize stats and init time
    memset(&s->stats, 0x00, sizeof(zdb_stats_t));
    s->stats.inittime = time(NULL);

    // initialize instance id
    s->iid = zdb_instanceid_generate();

    return &zdb_rootsettings;
}

zdb_settings_t *zdb_settings_get() {
    return &zdb_rootsettings;
}

void zdb_destroy(zdb_settings_t *zdb_settings) {
    zdb_debug("[+] bootstrap: cleaning library\n");
    free(zdb_settings->zdbid);
    zdb_settings->zdbid = NULL;
}

//
// tools
//

// returns running mode in readable string
char *zdb_running_mode(index_mode_t mode) {
    if(mode > (sizeof(zdb_modes) / sizeof(char *)))
        return "unsupported mode";

    return zdb_modes[mode];
}

// returns zdb string id
char *zdb_id() {
    if(!zdb_rootsettings.zdbid)
        return "unknown-id";

    return zdb_rootsettings.zdbid;
}

// set zdb id from string // FIXME
char *zdb_id_set(char *id) {
    zdb_rootsettings.zdbid = strdup(id);
    return zdb_rootsettings.zdbid;
}

// returns the instance id (generated during init)
uint32_t zdb_instanceid_get() {
    return zdb_rootsettings.iid;
}

// returns plain text version
char *zdb_version() {
    return ZDB_VERSION;
}

// returns plain text revision (git revision)
char *zdb_revision() {
    return ZDB_REVISION;
}
