#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <hiredis/hiredis.h>

void fatal(redisContext *context) {
    fprintf(stderr, "[-] %s\n", context->errstr);
    exit(EXIT_FAILURE);
}

int pipeline(redisContext *context) {
    redisReply *reply;
    size_t sets = 125;

    char *commands[] = {"PING", "INFO", "PING", "NSLIST", "DBSIZE", "TIME"};

    for(size_t i = 0; i < sizeof(commands) / sizeof(char *); i++) {
        printf("[+] executing: %s\n", commands[i]);
        redisAppendCommand(context, commands[i]);
    }

    for(size_t i = 0; i < sizeof(commands) / sizeof(char *); i++) {
        if(redisGetReply(context, (void **) &reply) != REDIS_OK)
            fatal(context);

        printf("[+] response okay\n");
        freeReplyObject(reply);
    }

    for(size_t i = 0; i < sets; i++) {
        redisAppendCommand(context, "DEL pipeline-%d", i);
        redisAppendCommand(context, "SET pipeline-%d THISISMYDATAHELLOYEAHWORKS", i);
    }

     for(size_t i = 0; i < sets * 2; i++) {
        if(redisGetReply(context, (void **) &reply) != REDIS_OK)
            fatal(context);

        printf("[+] response okay: %s\n", reply->str);
        freeReplyObject(reply);
    }

    return 0;
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

int main() {
    char *inhost = "localhost";
    int inport = 9900;
    redisContext *context;

    printf("[+] initializing hosts\n");

    if(!(context = initialize(inhost, inport)))
        exit(EXIT_FAILURE);

    pipeline(context);
    printf("[+] pipeline done\n");

    redisFree(context);
    return 0;
}
