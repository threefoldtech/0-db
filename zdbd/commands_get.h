#ifndef ZDB_COMMANDS_GET_H
    #define ZDB_COMMANDS_GET_H

    int command_get(redis_client_t *client);
    int command_mget(redis_client_t *client);
#endif
