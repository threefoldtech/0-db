#ifndef ZDB_COMMANDS_SCAN_H
    #define ZDB_COMMANDS_SCAN_H

    int command_scan(redis_client_t *client);
    int command_rscan(redis_client_t *client);
#endif
