#ifndef ZDB_COMMANDS_H
    #define ZDB_COMMANDS_H

    typedef struct command_t {
        char *command;
        int (*handler)(resp_request_t *request);

    } command_t;

    int redis_dispatcher(resp_request_t *request);
#endif
