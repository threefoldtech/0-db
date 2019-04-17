#ifndef __ZDB_SETTINGS_H
    #define __ZDB_SETTINGS_H

    zdb_settings_t *zdb_initialize();
    zdb_settings_t *zdb_settings_get();

    char *zdb_version();
    char *zdb_revision();

    char *zdb_running_mode(index_mode_t mode);

    char *zdb_id();
    char *zdb_id_set(char *listenaddr, int port, char *socket);

    uint32_t zdb_instanceid_get();
#endif
