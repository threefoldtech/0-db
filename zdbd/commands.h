#ifndef ZDB_COMMANDS_H
    #define ZDB_COMMANDS_H

    #define COMMAND_MAXLEN  256

    int redis_dispatcher(redis_client_t *client);

    int command_args_validate(redis_client_t *client, int expected);
    int command_args_validate_null(redis_client_t *client, int expected);
    int command_admin_authorized(redis_client_t *client);
    int command_wait(redis_client_t *client);
    int command_asterisk(redis_client_t *client);

    int command_error_locked(redis_client_t *client);
    int command_error_frozen(redis_client_t *client);
#endif
