#ifndef ZDB_COMMANDS_NAMESPACE_H
    #define ZDB_COMMANDS_NAMESPACE_H

    int command_nsnew(redis_client_t *client);
    int command_select(redis_client_t *client);
    int command_nslist(redis_client_t *client);
    int command_nsinfo(redis_client_t *client);
    int command_nsset(redis_client_t *client);
    int command_dbsize(redis_client_t *client);
#endif
