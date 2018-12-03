#ifndef __ZDB_FILESYSTEM_H
    #define __ZDB_FILESYSTEM_H

    int dir_exists(char *path);
    int dir_create(char *path);
    int dir_remove(char *path);
    int dir_clean_payload(char *path);
#endif
