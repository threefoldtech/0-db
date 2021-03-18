#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "libzdb.h"
#include "libzdb_private.h"

static uint32_t zdb_instanceid_generate() {
    struct timeval tv;

    // generating 'random id', greater than zero
    gettimeofday(&tv, NULL);
    srand((time_t) tv.tv_usec);

    return (uint32_t) ((rand() % (1 << 30)) + 1);
}

//
// main settings initializer
//
zdb_settings_t *zdb_initialize() {
    zdb_settings_t *s = &zdb_rootsettings;

    if(s->initialized == 1)
        return NULL;

    // apply default settings
    s->datapath = ZDB_DEFAULT_DATAPATH;
    s->indexpath = ZDB_DEFAULT_INDEXPATH;
    s->datasize = ZDB_DEFAULT_DATA_MAXSIZE;

    // running 0-db in mixed mode by default
    //
    // this flag can be used on runtime to specify
    // if mixed mode is allowed or not, if a specific
    // mode is set here, you could restrict instance to
    // a single mode, but this is up to caller to enable
    // restriction, library doesn't restrict anything
    s->mode = ZDB_MODE_MIX;

    // resetting values
    s->verbose = 0;
    s->dump = 0;
    s->sync = 0;
    s->synctime = 0;
    s->hook = NULL;
    s->maxsize = 0;

    // initialize stats and init time
    memset(&s->stats, 0x00, sizeof(zdb_stats_t));
    gettimeofday(&s->stats.inittime, NULL);

    // initialize hooks list
    hook_initialize(&s->hooks);

    // initialize instance id
    s->iid = zdb_instanceid_generate();

    // set a global lock, already initialized
    s->initialized = 1;

    return s;
}

zdb_settings_t *zdb_open(zdb_settings_t *zdb_settings) {
    //
    // ensure default directories exists
    // for a fresh start if this is a new instance
    //
    if(zdb_dir_exists(zdb_settings->datapath) != ZDB_DIRECTORY_EXISTS) {
        zdb_verbose("[+] system: creating datapath: %s\n", zdb_settings->datapath);
        zdb_dir_create(zdb_settings->datapath);
    }

    if(zdb_dir_exists(zdb_settings->indexpath) != ZDB_DIRECTORY_EXISTS) {
        zdb_verbose("[+] system: creating indexpath: %s\n", zdb_settings->indexpath);
        zdb_dir_create(zdb_settings->indexpath);
    }

    // namespace is the root of the whole index/data system
    // anything related to data is always attached to at least
    // one namespace (the default), and all the others
    // are based on a fork of namespace
    //
    // the namespace system will take care about all the loading
    // and the destruction
    namespaces_init(zdb_settings);

    return zdb_settings;
}

void zdb_close(zdb_settings_t *zdb_settings) {
    zdb_debug("[+] bootstrap: closing database\n");
    namespaces_destroy(zdb_settings);

    // cleanup hook subsystem
    hook_destroy(&zdb_settings->hooks);

    zdb_debug("[+] bootstrap: cleaning library\n");
    free(zdb_settings->zdbid);
    zdb_settings->zdbid = NULL;
    zdb_settings->initialized = 0;
}

