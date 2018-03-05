#ifndef ZDB_COMMANDS_NAMESPACE_H
    #define ZDB_COMMANDS_NAMESPACE_H

    int command_nsnew(resp_request_t *request);
    int command_select(resp_request_t *request);
    int command_nslist(resp_request_t *request);
    int command_nsinfo(resp_request_t *request);
    int command_nsset(resp_request_t *request);
    int command_dbsize(resp_request_t *request);
#endif
