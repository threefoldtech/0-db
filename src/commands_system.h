#ifndef ZDB_COMMANDS_SYSTEM_H
    #define ZDB_COMMANDS_SYSTEM_H

    int command_ping(resp_request_t *request);
    int command_time(resp_request_t *request);
    int command_auth(resp_request_t *request);
    int command_stop(resp_request_t *request);
    int command_info(resp_request_t *request);
#endif
