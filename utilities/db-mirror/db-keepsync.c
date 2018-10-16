#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <hiredis/hiredis.h>
#include <getopt.h>
#include <unistd.h>
#include "db-mirror.h"

#if 0
#define MB(x)   (x / (1024 * 1024.0))

static struct option long_options[] = {
    {"source", required_argument, 0, 's'},
    {"remote", required_argument, 0, 'r'},
    {"help",   no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

static char __hex[] = "0123456789abcdef";

void diep(char *str) {
    perror(str);
    exit(EXIT_FAILURE);
}

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

status_t warmup(sync_t *sync, char *namespace) {
    status_t status = {0};
    redisReply *reply;
    char *match;

    if(!(reply = redisCommand(sync->source, "NSINFO %s", namespace)))
        return status;

    if(!(match = strstr(reply->str, "data_size_bytes:"))) {
        // should not happen
        return status;
    }

    status.size = atol(match + 17);

    if(!(match = strstr(reply->str, "entries:"))) {
        // should not happen
        return status;
    }

    status.keys = atoi(match + 9);

    return status;
}

size_t keylist_append(keylist_t *keylist, redisReply *reply, size_t size) {
    if(keylist->allocated < keylist->length + 1) {
        size_t newsize = sizeof(redisReply *) * (keylist->allocated + 128);

        if(!(keylist->keys = (redisReply **) realloc(keylist->keys, newsize))) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }

        keylist->allocated += 128;
    }

    keylist->length += 1;
    keylist->size += size;
    keylist->keys[keylist->length - 1] = reply;

    return keylist->length;
}


int fetchsync(sync_t *sync, keylist_t *keylist, status_t *status) {
    redisReply *input, *output;

    for(size_t i = 0; i < keylist->length; i++) {
        if(redisGetReply(sync->source, (void **) &input) == REDIS_ERR) {
            fprintf(stderr, "\n[-] %s\n", sync->source->errstr);
            exit(EXIT_FAILURE);
        }

        char *key = keylist->keys[i]->element[0]->str;
        size_t keylen = keylist->keys[i]->element[0]->len;

        if(!(output = redisCommand(sync->targets[0], "SET %b %b", key, keylen, input->str, input->len)))
            return 0;

        status->transfered += input->len;
        status->copied += 1;

        float percent = (status->transfered / (double) status->size) * 100.0;
        printf("\r[+] syncing: % 3.1f %% [%lu/%lu keys, %.2f MB]", percent, status->requested, status->copied, MB(status->transfered));
        fflush(stdout);

        freeReplyObject(input);
        freeReplyObject(output);
        freeReplyObject(keylist->keys[i]);
    }

    return 0;
}

int replicate_from_reply(sync_t *sync, redisReply *from, status_t *status) {
    redisReply *reply = from;
    keylist_t keylist = {
        .length = 0,
        .keys = NULL,
        .size = 0,
        .allocated = 0,
    };

    while(reply && reply->type == REDIS_REPLY_ARRAY) {
        float percent = (status->transfered / (double) status->size) * 100.0;

        printf("\r[+] syncing: % 3.1f %% [%lu/%lu keys, %.2f MB]", percent, status->requested, status->copied, MB(status->transfered));
        fflush(stdout);

        for(size_t i = 0; i < reply->element[1]->elements; i++) {
            redisReply *item = reply->element[1]->element[i];

            // append the get in the buffer
            redisAppendCommand(sync->source, "GET %b", item->element[0]->str, item->element[0]->len);
            keylist_append(&keylist, item, item->element[1]->integer);


            // one more key requested
            status->requested += 1;
        }

        // query next key
        if(!(reply = redisCommand(sync->source, "SCAN %b", reply->element[0]->str, reply->element[0]->len)))
            return 1;

        // original reply will be free'd here
        // that's why new reply is already prepared ^
        fetchsync(sync, &keylist, status);
        keylist.length = 0;
        keylist.size = 0;
    }

    return 0;
}

int replicate(sync_t *sync) {
    redisReply *reply;
    printf("[+] preparing buffers\n");

    // loading stats
    printf("[+] preparing namespace\n");
    status_t status = warmup(sync, "default");

    printf("[+] namespace ready, %lu keys to transfert (%.2f MB)\n", status.keys, MB(status.size));

    if(!(reply = redisCommand(sync->source, "SCAN")))
        return 1;

    int value = replicate_from_reply(sync, reply, &status);

    printf("\n[+] initial synchronization done\n");
    return value;
}

int synchronize(sync_t *sync) {
    redisReply *reply, *wait, *from;
    status_t status = {0};

    printf("[+] synchronizing databases\n");

    while(1) {
        printf("[+] fetching last id\n");
        if(!(reply = redisCommand(sync->source, "RSCAN")))
            return 1;

        printf("[+] waiting trigger on master\n");
        if(!(wait = redisCommand(sync->source, "WAIT SET")))
            return 1;

        // wait is not needed
        freeReplyObject(wait);

        // fetching from last ket we know
        if(!(from = redisCommand(sync->source, "SCAN %b", reply->element[0]->str, reply->element[0]->len)))
            return 1;

        printf("[+] trigger fired, syncing...\n");
        replicate_from_reply(sync, from, &status);

        freeReplyObject(reply);
        printf("\n");
    }

    return 0;
}


char *forwarding[] = {"SET", "DEL", "NSNEW", "NSDEL", "NSSET"};

int forward(redisReply *reply, sync_t *sync) {
    redisReply *forwarding;
    redisReply **e = reply->element;
    char instance[32];

    sprintf(instance, "%u", (unsigned int) e[2]->integer);

    if(strcasecmp("SET", reply->element[3]->str) == 0) {
        if(!(forwarding = redisCommand(sync->targets[0], "SET %b %b %s", e[4]->str, e[4]->len, e[5]->str, e[5]->len, instance))) {
            printf("COULD NOT FORWARD\n");
            return 1;
        }

    } else {
        printf("FORWARD NOT SUPPORTED YET\n");
    }

    // printf("FORWARDING [%s]: <%x>\n", reply->element[2]->str, reply->element[3]->str[0] & 0xff);
    return 0;
}

int mirror(sync_t *sync) {
    redisReply *reply;
    char buffer[8192];
    ssize_t length;


    printf("[+] sending MIRROR request to master\n");
    if(!(reply = redisCommand(sync->source, "MIRROR")))
        return 1;

    printf("[+] response: %s\n", reply->str);
    freeReplyObject(reply);

    redisReader *reader = redisReaderCreate();

    while((length = read(sync->source->fd, buffer, sizeof(buffer)))) {
        // buffer[length] = '\0';
        // printf(">> %s\n", buffer);

        redisReaderFeed(reader, buffer, length);

        while(1) {
            redisReaderGetReply(reader, &reply);
            if(!reply)
                break;

            char *command = reply->element[3]->str;
            int sockfd = reply->element[0]->integer;
            char *namespace = reply->element[1]->str;
            uint32_t instance = (uint32_t) reply->element[2]->integer;

            printf("[+] command: <%s> from <%d> (ns: %s, iid: %u)\n", command, sockfd, namespace, instance);

            for(unsigned int i = 0; i < sizeof(forwarding) / sizeof(char *); i++)
                if(strcasecmp(forwarding[i], reply->element[3]->str) == 0)
                    forward(reply, sync);

            freeReplyObject(reply);
        }
    }

    return 1;
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
    // int value = mirror(&sync);


    // redisFree(sync.sourcep);
    // redisFree(sync.source);
    // redisFree(sync.target);

    return value;
}
#endif
