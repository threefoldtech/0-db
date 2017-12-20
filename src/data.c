#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <openssl/sha.h>
#include "rkv.h"
#include "data.h"

static data_t *rootdata = NULL;

char *sha256_hex(unsigned char *hash) {
    char *buffer = calloc((SHA256_DIGEST_LENGTH * 2) + 1, sizeof(char));

    for(int i = 0; i < SHA256_DIGEST_LENGTH; i++)
        sprintf(buffer + (i * 2), "%02x", hash[i]);

    return buffer;
}

unsigned char *sha256_compute(unsigned char *target, const char *buffer, size_t length) {
    const unsigned char *input = (const unsigned char *) buffer;
    SHA256_CTX sha256;

    SHA256_Init(&sha256);
    SHA256_Update(&sha256, input, length);
    SHA256_Final(target, &sha256);

    return target;
}

unsigned char *sha256_parse(char *buffer, unsigned char *target) {
    char temp[5] = "0xFF";

    for(int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        strncpy(temp + 2, buffer + (i * 2), 2);
        target[i] = strtol(temp, NULL, 16);
    }

    return target;
}

void data_init() {
   data_t *lroot = (data_t *) malloc(sizeof(data_t));

   lroot->datafile = "/tmp/rkv-data";
   // if((lroot->datafd = open(lroot->datafile, O_CREAT | O_SYNC | O_RDWR, 0600)) < 0)
   if((lroot->datafd = open(lroot->datafile, O_CREAT | O_RDWR, 0600)) < 0)
        diep(lroot->datafile);

   if(write(lroot->datafd, "X", 1) != 1)
       diep(lroot->datafile);

   rootdata = lroot;
}

char *data_get(size_t offset, size_t length) {
    char *buffer = malloc(length);

    lseek(rootdata->datafd, offset, SEEK_SET);
    if(read(rootdata->datafd, buffer, length) != (ssize_t) length) {
        fprintf(stderr, "[-] cannot read data\n");
        free(buffer);
        return NULL;
    }

    return buffer;
}

size_t data_insert(char *data, size_t length) {
    size_t offset = lseek(rootdata->datafd, 0, SEEK_CUR);

    if(write(rootdata->datafd, data, length) != (ssize_t) length) {
        fprintf(stderr, "[-] cannot write data\n");
        return 0; // data starts at 1, 0 cannot be used as offset
    }

    return offset;
}

void data_destroy() {
    free(rootdata);
}
