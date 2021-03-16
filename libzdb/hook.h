#ifndef __ZDB_HOOK_H
    #define __ZDB_HOOK_H

    #define ZDB_HOOKS_INITIAL_LENGTH  8
    #define ZDB_HOOKS_EXPIRE_SECONDS  300

    // a hook is called when an external application/script
    // is provided at runtime, this hook executable
    // will be executed in background for different actions
    typedef struct hook_t {
        pid_t pid;        // process id when started
        size_t argc;      // length or arguments
        char **argv;      // arguments
        time_t created;   // creation timestamp
        time_t finished;  // exit timestamp
        int status;       // exit status code

        size_t argidx;    // current argument index (used for fillin)

    } hook_t;

    typedef struct zdb_hooks_t {
        size_t length;
        size_t active;
        hook_t **hooks;

    } zdb_hooks_t;

    // initialize hook subsystem
    size_t hook_initialize(zdb_hooks_t *hooks);

    // cleanup hook subsystem
    void hook_destroy(zdb_hooks_t *hooks);

    hook_t *hook_new(char *name, size_t argc);
    int hook_append(hook_t *hook, char *argument);
    pid_t hook_execute(hook_t *hook);
    int hook_execute_wait(hook_t *hook);

    // need to be called periodicly to cleanup zombies
    void libzdb_hooks_cleanup();
#endif
