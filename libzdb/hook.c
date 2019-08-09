#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include "libzdb.h"
#include "libzdb_private.h"


hook_t *hook_new(char *name, size_t argc) {
    hook_t *hook;

    zdb_debug("[+] hook: creating hook <%s>\n", name);

    if(!(hook = malloc(sizeof(hook_t))))
        zdb_diep("hook: malloc");

    // we add +3 to args:
    //  - first is the script name (convention)
    //  - second is the type (hook name)
    //  - third is the last element set to NULL
    if(!(hook->argv = calloc(sizeof(char *), argc + 3)))
        zdb_diep("hook: calloc");

    if(!(hook->argv[0] = strdup(zdb_rootsettings.hook)))
        zdb_diep("hook: strdup");

    if(!(hook->argv[1] = strdup(name)))
        zdb_diep("hook: strdup");

    hook->argc = argc + 3;
    hook->argidx = 2;

    return hook;
}

int hook_append(hook_t *hook, char *argument) {
    if(hook->argidx == hook->argc - 1)
        return -1;

    if(!(hook->argv[hook->argidx] = strdup(argument)))
        zdb_diep("hook: append: strdup");

    hook->argidx += 1;

    return (int) hook->argidx;
}

int hook_execute(hook_t *hook) {
    pid_t pid;

    if((pid = fork()) != 0) {
        if(pid < 0)
            zdb_warnp("hook: fork");

        return 0;
    }

    // child process now
    zdb_debug("[+] hook: executing hook <%s> (%lu args)\n", hook->argv[0], hook->argc);

    execv(zdb_rootsettings.hook, hook->argv);
    zdb_warnp("hook: execv");

    return 1;
}

void hook_free(hook_t *hook) {
    // freeing all arguments
    for(size_t i = 0; i < hook->argc; i++)
        free(hook->argv[i]);

    // freeing objects
    free(hook->argv);
    free(hook);
}
