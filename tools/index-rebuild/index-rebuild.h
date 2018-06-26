#ifndef ZDB_TOOLS_INDEX_REBUILD_H
#define ZDB_TOOLS_INDEX_REBUILD_H

    typedef struct rebuild_t {
        char *datapath;
        char *indexpath;
        char *namespace;

    } rebuild_t;

    typedef struct buffer_t {
        char *buffer;      // the buffer memory area
        char *writer;      // writer for appending data
        size_t allocated;  // allocated bytes
        size_t length;     // used bytes on the buffer (by writer)

    } buffer_t;

    typedef struct datamap_entry_t {
        off_t offset;
        char keep;

    } datamap_entry_t;

    typedef struct datamap_t {
        size_t length;
        size_t allocated;
        uint16_t fileid;
        datamap_entry_t *entries;

    } datamap_t;

    void *warnp(char *str);
    void dies(char *str);
    void diep(char *str);

#endif
