#ifndef ZDB_COMMANDS_SCAN_H
    #define ZDB_COMMANDS_SCAN_H

    int command_scan(redis_client_t *client);
    int command_rscan(redis_client_t *client);

    typedef struct scan_list_t {
        size_t length;
        size_t allocated;
        index_item_t **items;
        uint32_t *offsets;

    } scan_list_t;

    typedef struct scan_info_t {
        uint16_t dataid;
        size_t idxoffset;

    } scan_info_t;

    // one call to SCAN/RSCAN can take up to
    // 2000 microseconds (2 milliseconds)
    #define SCAN_TIMESLICE_US  2000

#endif
