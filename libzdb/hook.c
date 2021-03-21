#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdint.h>
#include <time.h>
#include "libzdb.h"
#include "libzdb_private.h"

static void hook_free(hook_t *hook) {
    // freeing all arguments
    for(size_t i = 0; i < hook->argc; i++)
        free(hook->argv[i]);

    // freeing objects
    free(hook->argv);
    free(hook);
}

size_t hook_initialize(zdb_hooks_t *hooks) {
    // initialize default list length
    hooks->length = ZDB_HOOKS_INITIAL_LENGTH;
    hooks->active = 0;

    // initialize default master list
    if(!(hooks->hooks = (hook_t **) calloc(sizeof(hook_t *), hooks->length)))
        return 0;

    return hooks->length;
}

void hook_destroy(zdb_hooks_t *hooks) {
    // cleanup any hook on the list
    for(size_t i = 0; i < hooks->length; i++) {
        if(hooks->hooks[i])
            hook_free(hooks->hooks[i]);
    }

    // cleanup master list
    free(hooks->hooks);
}


static hook_t *hooks_append_hook(zdb_hooks_t *hooks, hook_t *hook) {
    zdb_debug("[+] hooks: looking for an empty slot\n");

    for(size_t i = 0; i < hooks->length; i++) {
        if(hooks->hooks[i] == NULL) {
            hooks->active += 1;
            hooks->hooks[i] = hook;
            return hook;
        }
    }

    // no slot available, growing up list
    size_t next = hooks->length;
    hooks->length = hooks->length * 2;

    zdb_debug("[+] hooks: growing up hooks list (%lu slots)\n", hooks->length);

    if(!(hooks->hooks = realloc(hooks->hooks, sizeof(hook_t *) * hooks->length)))
        return NULL;

    // initialize new allocated slots
    for(size_t i = next; i < hooks->length; i++)
        hooks->hooks[i] = NULL;

    hooks->hooks[next] = hook;
    hooks->active += 1;

    return hook;
}

hook_t *hook_new(char *name, size_t argc) {
    hook_t *hook;

    zdb_debug("[+] hook: creating hook <%s>\n", name);

    if(!(hook = calloc(sizeof(hook_t), 1)))
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

    // FIXME: should not return the hook, should support error
    if(!(hooks_append_hook(&zdb_rootsettings.hooks, hook)))
        return hook;

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

pid_t hook_execute(hook_t *hook) {
    pid_t pid;

    hook->created = time(NULL);

    if((pid = fork()) != 0) {
        if(pid < 0) {
            zdb_warnp("hook: fork");
            pid = 0;

        } else {
            // adding one pending child
            zdb_rootsettings.stats.childwait += 1;
        }

        hook->pid = pid;
        return pid;
    }

    // child process now
    zdb_debug("[+] hooks: executing hook <%s> (%lu args)\n", hook->argv[0], hook->argc);

    execv(zdb_rootsettings.hook, hook->argv);
    zdb_warnp("hook: execv");

    exit(EXIT_FAILURE);
    return 0;
}

int hook_execute_wait(hook_t *hook) {
    pid_t child;
    int status;

    child = hook_execute(hook);
    zdb_debug("[+] hooks: waiting for hook to finish: %d\n", child);

    if(waitpid(child, &status, 0) < 0)
        zdb_warnp("waitpid");

    hook->finished = time(NULL);
    hook->status = WEXITSTATUS(status);

    // update stats, since we wait, this won't be handled
    // by recurring scrubber
    zdb_rootsettings.stats.childwait -= 1;

    return hook->status;
}

static void hook_expired_cleanup(zdb_hooks_t *hooks) {
    time_t now = time(NULL);

    for(size_t i = 0; i < hooks->length; i++) {
        hook_t *hook = hooks->hooks[i];

        // empty slot, nothing to do
        if(hook == NULL)
            continue;

        // task not finished yet
        if(hook->finished == 0)
            continue;

        // 1 minute expiration
        if(hook->finished < now - ZDB_HOOKS_EXPIRE_SECONDS) {
            zdb_debug("[+] hooks: cleaning expired hook [%d]\n", hook->pid);

            // cleanup this hook
            hook_free(hook);

            // drop this hook from the list
            hooks->hooks[i] = NULL;
            hooks->active -= 1;
        }
    }
}

void libzdb_hooks_cleanup() {
    zdb_hooks_t *hooks = &zdb_rootsettings.hooks;
    int status;
    pid_t pid;

    // nothing to do if hooks are disabled
    if(!zdb_rootsettings.hook)
        return;

    // no pending child
    if(zdb_rootsettings.stats.childwait == 0) {
        // no hooks active at all, nothing to do
        if(hooks->active == 0)
            return;

        // there are some active hooks on the list
        // let's see if any of them expired and needs
        // some cleanup
        hook_expired_cleanup(hooks);
        return;
    }

    // some child were executed, check if they are done
    // if nothing are done, just wait next time
    while((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        zdb_debug("[+] hooks: pid %d terminated, status: %d\n", pid, WEXITSTATUS(status));

        // one child terminated
        if(WIFEXITED(status) || WIFSIGNALED(status)) {
            zdb_rootsettings.stats.childwait -= 1;
        }

        // looking for matching hook
        for(size_t i = 0; i < hooks->length; i++) {
            hook_t *hook = hooks->hooks[i];

            // only match same pid and running (not finished) process
            if(hook->pid == pid && hook->finished == 0) {
                hook->finished = time(NULL);
                hook->status = WEXITSTATUS(status);
                break;
            }
        }
    }
}
