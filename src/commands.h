#ifndef ZDB_COMMANDS_H
    #define ZDB_COMMANDS_H

    typedef struct command_t {
        char *command;
        int (*handler)(resp_request_t *request);

    } command_t;

    #define COMMAND_MAXLEN  256

    int redis_dispatcher(resp_request_t *request);

    int command_args_validate(resp_request_t *request, int expected);
    int command_admin_authorized(resp_request_t *request);
#endif
