#ifndef __ZDB_FILESYSTEM_H
    #define __ZDB_FILESYSTEM_H

    int dir_exists(char *path);
    int dir_create(char *path);
    int dir_remove(char *path);
    int dir_clean_payload(char *path);

    typedef struct filebuf_t {
        char *buffer;
        size_t allocated;
        ssize_t length;
        off_t nextoff;

    } filebuf_t;

    filebuf_t *file_dump(char *filename, off_t offset, off_t maxlength);
    filebuf_t *file_write(char *filename, off_t offset, filebuf_t *buffer);
#endif
