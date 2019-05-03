#ifndef __ZDB_FILESYSTEM_H
    #define __ZDB_FILESYSTEM_H

    int zdb_dir_exists(char *path);
    int zdb_dir_create(char *path);
    int zdb_dir_remove(char *path);
    int zdb_dir_clean_payload(char *path);
    int zdb_file_exists(char *path);

    #define ZDB_FILE_EXISTS             0
    #define ZDB_DIRECTORY_EXISTS        1
    #define ZDB_PATH_NOT_AVAILABLE      2
    #define ZDB_PATH_IS_DIRECTORY       3
    #define ZDB_PATH_IS_NOT_DIRECTORY   4
#endif
