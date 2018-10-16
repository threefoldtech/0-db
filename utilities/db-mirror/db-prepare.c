#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <hiredis/hiredis.h>
#include <getopt.h>
#include <unistd.h>
#include "db-mirror.h"

namespace_t *warmup(sync_t *sync, namespace_t *namespace) {
    redisReply *reply;
    char *match, *temp;

    printf("[+] fetching namespace information: %s\n", namespace->name);

    if(!(reply = redisCommand(sync->source, "NSINFO %s 0", namespace->name)))
        return NULL;

    if((match = strstr(reply->str, "mode: seq"))) {
        printf("[-] namespace running in sequential mode, replication not supported\n");
        exit(EXIT_FAILURE);
    }

    if(!(match = strstr(reply->str, "data_size_bytes:")))
        return NULL;

    namespace->status.size = atol(match + 17);

    if(!(match = strstr(reply->str, "entries:")))
        return NULL;

    namespace->status.keys = atoi(match + 9);

    namespace->public = strstr(reply->str, "public: yes") ? 1 : 0;

    if(strstr(reply->str, "password: yes")) {
        if(!(match = strstr(reply->str, "password_raw: ")))
            return NULL;

        match += 14;
        temp = strchr(match, '\n');

        if(!(namespace->password = strndup(match, temp - match)))
            diep("strdup");
    }

    printf("[+]   size: %.2f MB\n", MB(namespace->status.size));
    printf("[+]   keys: %lu\n", namespace->status.keys);
    printf("[+]   pass: %s\n", namespace->password);

    return namespace;
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
    namespaces_t namespaces = {
        .length = 0,
        .list = NULL
    };

    printf("[+] preparing buffers\n");

    //
    // fetching list of namespaces
    //
    printf("[+] preparing namespaces\n");
    if(!(reply = redisCommand(sync->source, "NSLIST 0")))
        return 1;

    namespaces.length = reply->elements;
    if(!(namespaces.list = (namespace_t *) calloc(sizeof(namespace_t), namespaces.length)))
        diep("calloc");

    for(unsigned int i = 0; i < reply->elements; i++) {
        if(!(namespaces.list[i].name = strdup(reply->element[i]->str)))
            diep("strdup");
    }

    freeReplyObject(reply);

    //
    // fetching namespaces informations
    //
    for(unsigned int i = 0; i < namespaces.length; i++) {
        namespace_t *namespace = &namespaces.list[i];
        if(!(warmup(sync, namespace))) {
            fprintf(stderr, "[-] could not fetch namespace information\n");
            exit(EXIT_FAILURE);
        }
    }

    exit(1);

    // printf("[+] namespace ready, %lu keys to transfert (%.2f MB)\n", status.keys, MB(status.size));

    if(!(reply = redisCommand(sync->source, "SCAN")))
        return 1;

    // int value = replicate_from_reply(sync, reply, &status);

    printf("\n[+] initial synchronization done\n");
    return 0;
}

