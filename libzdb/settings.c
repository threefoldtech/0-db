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

//
// public settings accessor
//
zdb_settings_t *zdb_settings_get() {
    return &zdb_rootsettings;
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
