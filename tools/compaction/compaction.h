#ifndef ZDB_TOOLS_COMPACTION_H
#define ZDB_TOOLS_COMPACTION_H

    typedef struct compaction_t {
        char *datapath;
        char *indexpath;
        char *namespace;

    } compaction_t;


    typedef struct datamap_entry_t {
        uint16_t fileid;
        off_t offset;

    } datamap_entry_t;

    typedef struct datamap_t {
        size_t length;
        datamap_entry_t **entries[];

    } datamap_t;

    void *warnp(char *str);
    void diep(char *str);

#endif
