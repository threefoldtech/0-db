#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <hiredis/hiredis.h>
#include <getopt.h>
#include <unistd.h>
#include "db-mirror.h"

static struct option long_options[] = {
    {"source", required_argument, 0, 's'},
    {"remote", required_argument, 0, 'r'},
    {"help",   no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

void diep(char *str) {
    perror(str);
    exit(EXIT_FAILURE);
}

static char __hex[] = "0123456789abcdef";

void hexdump(void *input, size_t length) {
    unsigned char *buffer = (unsigned char *) input;
    char *output = calloc((length * 2) + 1, 1);
    char *writer = output;

    for(unsigned int i = 0, j = 0; i < length; i++, j += 2) {
        *writer++ = __hex[(buffer[i] & 0xF0) >> 4];
        *writer++ = __hex[buffer[i] & 0x0F];
    }

    printf("0x%s", output);
    free(output);
}

redisContext *initialize(char *hostname, int port) {
    struct timeval timeout = {5, 0};
    redisContext *context;

    printf("[+] connecting: %s, port: %d\n", hostname, port);

    if(!(context = redisConnectWithTimeout(hostname, port, timeout)))
        return NULL;

    if(context->err) {
        fprintf(stderr, "[-] redis error: %s\n", context->errstr);
        return NULL;
    }

    return context;
}

redisContext *mkhost(char *argument) {
    int port = 9900;
    char *hostname = NULL;
    char *password = NULL;
    char *match, *temp;

    // FIXME: memory leak on hostname

    // do we have a port specified
    if((match = strchr(argument, ':'))) {
        // does this port is before coma
        // otherwise the colon could be on
        // the password
        if((temp = strchr(argument, ','))) {
            // if coma is before colon, it's okay
            // it's a port specification
            if(temp > match) {
                port = atoi(match + 1);
                hostname = strndup(argument, match - argument);
            }

        } else {
            port = atoi(match + 1);
            hostname = strndup(argument, match - argument);
        }
    }

    if((match = strchr(argument, ','))) {
        password = match + 1;

        if(!hostname)
            hostname = strndup(argument, match - argument);
    }

    // nothing more than the hostname was specified
    if(!hostname)
        hostname = strdup(argument);

    // connect to the database
    redisContext *ctx;
    redisReply *auth;

    if(!(ctx = initialize(hostname, port)))
        return NULL;

    if(password) {
        if(!(auth = redisCommand(ctx, "AUTH %s", password))) {
            fprintf(stderr, "[-] %s:%d: could not send AUTH command to server\n", hostname, port);
            return NULL;
        }

        if(strcmp(auth->str, "OK")) {
            fprintf(stderr, "[-] %s:%d: could not authenticate: %s\n", hostname, port, auth->str);
        }

        freeReplyObject(auth);
    }

    printf("[+] sending MASTER request to database\n");
    if(!(auth = redisCommand(ctx, "MASTER")))
        return NULL;

    printf("[+] response: %s\n", auth->str);
    freeReplyObject(auth);

    printf("[+] %s:%d: database connected\n", hostname, port);
    return ctx;
}

redisContext *appendhost(sync_t *sync, char *argument) {
    int index = sync->remotes;
    sync->remotes += 1;

    if(!(sync->targets = realloc(sync->targets, sizeof(redisContext *) * sync->remotes)))
        diep("realloc");

    sync->targets[index] = mkhost(argument);
    return sync->targets[index];
}

void usage(char *program) {
    printf("%s: synchronize two 0-db database\n\n", program);

    printf("Available options:\n");
    printf("  --source  host[:port[,password]]    host parameter for source database\n");
    printf("  --remote  host[:port[,password]]    host parameter for target database\n\n");
    printf("  --help      this message (implemented)\n\n");

    printf("Only one source can be provided and is required\n");
    printf("Multiple target can be set, at least one is required\n");
}

int main(int argc, char **argv) {
    int option_index = 0;
    sync_t sync = {
        .source = NULL,
        .remotes = 0,
        .targets = NULL,
    };

    while(1) {
        int i = getopt_long_only(argc, argv, "", long_options, &option_index);

        if(i == -1)
            break;

        switch(i) {
            case 's':
                if(sync.source) {
                    fprintf(stderr, "[-] multiple source provided\n");
                    exit(EXIT_FAILURE);
                }

                sync.source = mkhost(optarg);
                break;

            case 'r':
                appendhost(&sync, optarg);
                break;

            case 'h':
                usage(argv[0]);
                exit(EXIT_FAILURE);

            case '?':
            default:
               exit(EXIT_FAILURE);
        }
    }

    if(!sync.source) {
        fprintf(stderr, "[-] missing source host\n");
        exit(EXIT_FAILURE);
    }

    if(sync.remotes == 0) {
        fprintf(stderr, "[-] missing at least one target host\n");
        exit(EXIT_FAILURE);
    }

    int value = 0;

    // initial replication
    replicate(&sync);

    // int value = mirror(&sync);


    // redisFree(sync.sourcep);
    // redisFree(sync.source);
    // redisFree(sync.target);

    return value;
}
