#ifndef __APPLE__
#define _XOPEN_SOURCE 500
#endif
#define _DEFAULT_SOURCE
#define _BSD_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <ftw.h>
#include "libzdb.h"
#include "libzdb_private.h"

//
// system directory management
//
int zdb_dir_exists(char *path) {
    struct stat sb;

    if(stat(path, &sb) != 0)
        return ZDB_PATH_NOT_AVAILABLE;

    if(!S_ISDIR(sb.st_mode))
        return ZDB_PATH_IS_NOT_DIRECTORY;

    return ZDB_DIRECTORY_EXISTS;
}

int zdb_dir_create(char *path) {
    char tmp[ZDB_PATH_MAX], *p = NULL;
    size_t len;

    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    if(tmp[len - 1] == '/')
        tmp[len - 1] = 0;

    for(p = tmp + 1; *p; p++) {
        if(*p == '/') {
            *p = 0;
            mkdir(tmp, S_IRWXU);
            *p = '/';
        }
    }

    return mkdir(tmp, S_IRWXU);
}

static int dir_remove_cb(const char *fpath, const struct stat *sb, int tflag, struct FTW *ftwbuf) {
    (void) sb;
    (void) ftwbuf;
    char *fullpath = (char *) fpath;
    int value;

    zdb_debug("[+] filesystem: remove: %s\n", fullpath);

    if((value = remove(fullpath)))
        zdb_warnp(fullpath);

    return tflag;
}

int zdb_dir_remove(char *path) {
    return nftw(path, dir_remove_cb, 64, FTW_DEPTH | FTW_PHYS);
}

static int dir_clean_cb(const char *fpath, const struct stat *sb, int tflag, struct FTW *ftwbuf) {
    (void) sb;
    (void) ftwbuf;
    char *fullpath = (char *) fpath;
    size_t length = strlen(fullpath);

    if(strncmp(fullpath + length - 14, "zdb-data-", 9) == 0) {
        zdb_debug("[+] filesystem: removing datafile: %s\n", fullpath);
        remove(fullpath);
    }

    if(strncmp(fullpath + length - 15, "zdb-index-", 10) == 0) {
        zdb_debug("[+] filesystem: removing indexfile: %s\n", fullpath);
        remove(fullpath);
    }

    return tflag;
}

int zdb_dir_clean_payload(char *path) {
    return nftw(path, dir_clean_cb, 64, FTW_DEPTH | FTW_PHYS);
}

int zdb_file_exists(char *path) {
    struct stat sb;

    if(stat(path, &sb) != 0)
        return ZDB_PATH_NOT_AVAILABLE;

    if(S_ISDIR(sb.st_mode))
        return ZDB_PATH_IS_DIRECTORY;

    return ZDB_FILE_EXISTS;
}
