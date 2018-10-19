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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
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

static int dir_remove_cb(const char *fpath, const struct stat *sb, int tflag, struct FTW *ftwbuf) {
    (void) sb;
    (void) ftwbuf;
    char *fullpath = (char *) fpath;
    int value;

    debug("[+] filesystem: remove: %s\n", fullpath);

    if((value = remove(fullpath)))
        warnp(fullpath);

    return tflag;
}

int dir_remove(char *path) {
    return nftw(path, dir_remove_cb, 64, FTW_DEPTH | FTW_PHYS);
}

static int dir_clean_cb(const char *fpath, const struct stat *sb, int tflag, struct FTW *ftwbuf) {
    (void) sb;
    (void) ftwbuf;
    char *fullpath = (char *) fpath;
    size_t length = strlen(fullpath);

    if(strncmp(fullpath + length - 14, "zdb-data-", 9) == 0) {
        debug("[+] filesystem: removing datafile: %s\n", fullpath);
        remove(fullpath);
    }

    if(strncmp(fullpath + length - 15, "zdb-index-", 10) == 0) {
        debug("[+] filesystem: removing indexfile: %s\n", fullpath);
        remove(fullpath);
    }

    return tflag;
}

int dir_clean_payload(char *path) {
    return nftw(path, dir_clean_cb, 64, FTW_DEPTH | FTW_PHYS);
}

static void *file_dump_clean(filebuf_t *buffer, int fd, char *error) {
    warnp(error);

    free(buffer->buffer);
    free(buffer);
    close(fd);

    return NULL;
}

filebuf_t *file_dump(char *filename, off_t offset, off_t maxlength) {
    filebuf_t *buffer;
    int fd;

    if((fd = open(filename, O_RDONLY)) < 0) {
        warnp(filename);
        return NULL;
    }

    if(!(buffer = malloc(sizeof(filebuf_t))))
        return NULL;

    off_t maxoff = lseek(fd, 0, SEEK_END);

    // does the file is larger than expected buffer
    buffer->allocated = (maxoff - offset < maxlength) ? maxoff - offset : maxlength;
    if(!(buffer->buffer = malloc(buffer->allocated)))
        return file_dump_clean(buffer, fd, "malloc");

    lseek(fd, offset, SEEK_SET);

    if((buffer->length = read(fd, buffer->buffer, buffer->allocated)) < 0)
        return file_dump_clean(buffer, fd, filename);

    buffer->nextoff = offset + buffer->length;
    close(fd);

    return buffer;
}
