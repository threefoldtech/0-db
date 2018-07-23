#ifndef ZDB_COMMANDS_H
    #define ZDB_COMMANDS_H

    typedef struct command_t {
        char *command;
        int (*handler)(redis_client_t *client);

    } command_t;

    #define COMMAND_MAXLEN  256

    int redis_dispatcher(redis_client_t *client);

    int command_args_validate(redis_client_t *client, int expected);
    int command_args_validate_null(redis_client_t *client, int expected);
    int command_admin_authorized(redis_client_t *client);
    int command_wait(redis_client_t *client);
#endif
