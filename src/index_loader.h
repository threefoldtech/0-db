#ifndef __ZDB_INDEX_LOADER_H
    #define __ZDB_INDEX_LOADER_H

    // initialize index header file
    index_t index_initialize(int fd, uint16_t indexid);

    // initialize the whole index system
    uint16_t index_init(settings_t *settings);

    void index_destroy();
#endif
