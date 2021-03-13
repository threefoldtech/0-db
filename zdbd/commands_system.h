#ifndef ZDB_COMMANDS_SYSTEM_H
    #define ZDB_COMMANDS_SYSTEM_H

    int command_ping(redis_client_t *client);
    int command_time(redis_client_t *client);
    int command_auth(redis_client_t *client);
    int command_stop(redis_client_t *client);
    int command_info(redis_client_t *client);

    int command_hooks(redis_client_t *client);
    int command_index(redis_client_t *client);
#endif
