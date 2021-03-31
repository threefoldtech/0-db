#ifndef ZDB_COMMANDS_SCAN_H
    #define ZDB_COMMANDS_SCAN_H

    int command_scan(redis_client_t *client);
    int command_rscan(redis_client_t *client);
    int command_keycur(redis_client_t *client);
    int command_kscan(redis_client_t *client);

    typedef struct scan_info_t {
        fileid_t dataid;
        fileid_t idxid;
        size_t idxoffset;

    } scan_info_t;

    typedef struct scan_list_t {
        size_t length;
        size_t allocated;
        index_item_t **items;
        scan_info_t *scansinfo;

    } scan_list_t;

    typedef struct list_t {
        void **items;
        size_t length;
        size_t allocated;

    } list_t;

    // one call to SCAN/RSCAN can take up to
    // 2000 microseconds (2 milliseconds)
    #define SCAN_TIMESLICE_US  2000

#endif
