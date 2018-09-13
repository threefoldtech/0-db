#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <hiredis/hiredis.h>
#include <getopt.h>
#include <unistd.h>

#define MB(x)   (x / (1024 * 1024.0))

static struct option long_options[] = {
    {"source-host", required_argument, 0, 's'},
    {"source-port", required_argument, 0, 'p'},
    {"remote-host", required_argument, 0, 'r'},
    {"remote-port", required_argument, 0, 'P'},
    {"password",    required_argument, 0, 'x'},
    {"help",        no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

typedef struct sync_t {
    redisContext *sourceq;  // source query
    redisContext *sourcep;  // source payload
    redisContext *target;

} sync_t;


//
// interface
//
typedef struct status_t {
    size_t size;        // total in bytes, to transfert
    size_t transfered;  // total in bytes transfered

    size_t keys;        // amount of keys to transfert
    size_t copied;      // amount of keys transfered
    size_t requested;   // amount of keys requested

} status_t;

typedef struct keylist_t {
    size_t length;
    size_t allocated;
    size_t size;
    redisReply **keys;

} keylist_t;

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

status_t warmup(sync_t *sync, char *namespace) {
    status_t status = {0};
    redisReply *reply;
    char *match;

    if(!(reply = redisCommand(sync->sourceq, "NSINFO %s", namespace)))
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
        if(redisGetReply(sync->sourcep, (void **) &input) == REDIS_ERR) {
            fprintf(stderr, "\n[-] %s\n", sync->sourcep->errstr);
            exit(EXIT_FAILURE);
        }

        char *key = keylist->keys[i]->element[0]->str;
        size_t keylen = keylist->keys[i]->element[0]->len;

        if(!(output = redisCommand(sync->target, "SET %b %b", key, keylen, input->str, input->len)))
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
            redisAppendCommand(sync->sourcep, "GET %b", item->element[0]->str, item->element[0]->len);
            keylist_append(&keylist, item, item->element[1]->integer);


            // one more key requested
            status->requested += 1;
        }

        // query next key
        if(!(reply = redisCommand(sync->sourceq, "SCAN %b", reply->element[0]->str, reply->element[0]->len)))
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

    if(!(reply = redisCommand(sync->sourceq, "SCAN")))
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
        if(!(reply = redisCommand(sync->sourceq, "RSCAN")))
            return 1;

        printf("[+] waiting trigger on master\n");
        if(!(wait = redisCommand(sync->sourceq, "WAIT SET")))
            return 1;

        // wait is not needed
        freeReplyObject(wait);

        // fetching from last ket we know
        if(!(from = redisCommand(sync->sourceq, "SCAN %b", reply->element[0]->str, reply->element[0]->len)))
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
    // printf("FORWARDING [%s]: <%x>\n", reply->element[2]->str, reply->element[3]->str[0] & 0xff);
    return 0;
}

int mirror(sync_t *sync) {
    redisReply *reply;
    char buffer[8192];
    ssize_t length;

    printf("[+] sending MIRROR request\n");
    if(!(reply = redisCommand(sync->sourceq, "MIRROR")))
        return 1;

    printf("[+] response: %s\n", reply->str);
    freeReplyObject(reply);

    redisReader *reader = redisReaderCreate();

    while((length = read(sync->sourceq->fd, buffer, sizeof(buffer)))) {
        // buffer[length] = '\0';
        // printf(">> %s\n", buffer);

        redisReaderFeed(reader, buffer, length);

        while(1) {
            redisReaderGetReply(reader, &reply);

            if(!reply)
                break;

            char *command = reply->element[2]->str;
            int sockfd = reply->element[0]->integer;
            char *namespace = reply->element[1]->str;

            printf("[+] command: <%s> from <%d> (namespace: %s)\n", command, sockfd, namespace);

            for(int i = 0; i < sizeof(forwarding) / sizeof(char *); i++)
                if(strcasecmp(forwarding[i], reply->element[2]->str) == 0)
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

void usage(char *program) {
    printf("%s: synchronize two 0-db database\n\n", program);

    printf("Available options:\n");
    printf("  --source-host      hostname of source database (required)\n");
    printf("  --source-port      port number of source database (default 9900)\n");
    printf("  --remote-host      hostname of target database (required)\n");
    printf("  --remote-port      port number of target database (default 9900)\n\n");

    printf("  --namespace        specify namespace to sync (not implemented)\n");
    printf("  --password         password to reach namespace (not implemented\n");
    printf("  --help             this message (implemented)\n");
}

int main(int argc, char **argv) {
    sync_t sync;
    int option_index = 0;
    char *inhost = NULL, *outhost = NULL;
    int inport = 9900, outport = 9900;

    while(1) {
        int i = getopt_long_only(argc, argv, "", long_options, &option_index);

        if(i == -1)
            break;

        switch(i) {
            case 's':
                inhost = optarg;
                break;

            case 'p':
                inport = atoi(optarg);
                break;

            case 'r':
                outhost = optarg;
                break;

            case 'P':
                outport = atoi(optarg);
                break;

            case 'h':
                usage(argv[0]);
                exit(EXIT_FAILURE);

            case '?':
            default:
               exit(EXIT_FAILURE);
        }
    }

    if(!inhost) {
        fprintf(stderr, "[-] missing input host\n");
        exit(EXIT_FAILURE);
    }

    if(!outhost) {
        fprintf(stderr, "[-] missing output host\n");
        exit(EXIT_FAILURE);
    }

    printf("[+] initializing hosts\n");

    if(!(sync.sourcep = initialize(inhost, inport)))
        exit(EXIT_FAILURE);

    if(!(sync.sourceq = initialize(inhost, inport)))
        exit(EXIT_FAILURE);

    if(!(sync.target = initialize(outhost, outport)))
        exit(EXIT_FAILURE);

    // synchronize databases
    // int value = replicate(&sync);

    // if(value == 0)
        // value = synchronize(&sync);

    int value = mirror(&sync);

    redisFree(sync.sourcep);
    redisFree(sync.sourceq);
    redisFree(sync.target);

    return value;
}
