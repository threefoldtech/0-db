#ifndef ZDB_TOOLS_COMPACTION_H
#define ZDB_TOOLS_COMPACTION_H

    typedef struct compaction_t {
        char *datapath;
        char *targetpath;
        char *namespace;

    } compaction_t;


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
    void diep(char *str);
    void dies(char *str);
#endif
