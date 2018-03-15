#ifndef ZDB_COMMANDS_GET_H
    #define ZDB_COMMANDS_GET_H

    int command_get(redis_client_t *client);

    extern index_entry_t * (*redis_get_handlers[])(redis_client_t *client);
#endif
