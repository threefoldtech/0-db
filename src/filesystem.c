#define _XOPEN_SOURCE 500
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <ftw.h>
#include "filesystem.h"
#include "zerodb.h"

//
// system directory management
//
int dir_exists(char *path) {
    struct stat sb;
    return (stat(path, &sb) == 0 && S_ISDIR(sb.st_mode));
}

int dir_create(char *path) {
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

int dir_remove_cb(const char *fpath, const struct stat *sb, int tflag, struct FTW *ftwbuf) {
    (void) sb;
    (void) ftwbuf;
    (void) tflag;
    char *fullpath = (char *) fpath;
    int value;

    debug("[+] filesystem: remove: %s\n", fullpath);

    // overwrite tflag to use it (lulz)
    if((value = remove(fullpath)))
        warnp(fullpath);

    return tflag;
}

int dir_remove(char *path) {
    return nftw(path, dir_remove_cb, 64, FTW_DEPTH | FTW_PHYS);
}
