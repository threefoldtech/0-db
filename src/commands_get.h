#ifndef ZDB_COMMANDS_GET_H
    #define ZDB_COMMANDS_GET_H

    int command_get(resp_request_t *request);

    extern index_entry_t * (*redis_get_handlers[ZDB_MODES])(resp_request_t *request);
#endif
