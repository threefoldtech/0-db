#ifndef ZDB_TOOLS_COMPACTION_H
#define ZDB_TOOLS_COMPACTION_H


    typedef struct datamap_entry_t {
        off_t offset;   // offset on source file
        size_t length;  // full entry length (header + id + payload)
        char keep;      // do we need to keep this entry or not

    } datamap_entry_t;

    typedef struct datamap_t {
        size_t length;
        size_t allocated;
        fileid_t fileid;
        datamap_entry_t *entries;

    } datamap_t;

    typedef struct compaction_t {
        char *datapath;
        char *targetpath;
        char *namespace;

        datamap_t **filesmap;

    } compaction_t;

    void *warnp(char *str);
    void diep(char *str);
    void dies(char *str);
#endif
