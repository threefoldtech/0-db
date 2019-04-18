#ifndef __ZDB_BOOTSTRAP_H
    #define __ZDB_BOOTSTRAP_H

    zdb_settings_t *zdb_initialize();
    zdb_settings_t *zdb_open(zdb_settings_t *zdb_settings);
    void zdb_close(zdb_settings_t *zdb_settings);
#endif
