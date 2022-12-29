#ifndef ZDB_COMMANDS_H
    #define ZDB_COMMANDS_H

    #define COMMAND_MAXLEN  256

    int redis_dispatcher(redis_client_t *client);

    int command_args_validate(redis_client_t *client, int expected);
    int command_args_validate_min(redis_client_t *client, int expected);
    int command_args_validate_null(redis_client_t *client, int expected);

    // avoid too long argument
    int command_args_overflow(redis_client_t *client, int argidx, int maxlen);

    // validate a argument as namespace
    int command_args_namespace(redis_client_t *client, int argidx);

    int command_admin_authorized(redis_client_t *client);
    int command_wait(redis_client_t *client);
    int command_asterisk(redis_client_t *client);

    int command_error_locked(redis_client_t *client);
    int command_error_frozen(redis_client_t *client);

    // defined in commands_set.c
    // used by commands_set.c and commands_dataset.c
    time_t timestamp_from_set(resp_request_t *request, int field);
#endif
