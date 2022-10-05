#ifndef ZDB_COMMANDS_DATASET_H
    #define ZDB_COMMANDS_DATASET_H

    int command_exists(redis_client_t *client);
    int command_check(redis_client_t *client);
    int command_del(redis_client_t *client);
    int command_length(redis_client_t *client);
#endif
