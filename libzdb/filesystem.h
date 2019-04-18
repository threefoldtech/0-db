#ifndef __ZDB_FILESYSTEM_H
    #define __ZDB_FILESYSTEM_H

    int zdb_dir_exists(char *path);
    int zdb_dir_create(char *path);
    int zdb_dir_remove(char *path);
    int zdb_dir_clean_payload(char *path);
#endif
