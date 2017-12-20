#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <openssl/sha.h>
#include "rkv.h"
#include "data.h"

static data_t *rootdata = NULL;

//
// hash functions
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

//
// data management
//
void data_initialize(char *filename) {
    int fd;

    if((fd = open(filename, O_CREAT | O_RDWR, 0600)) < 0)
        diep(filename);

    // writing initial header
    if(write(fd, "X", 1) != 1)
        diep(filename);

    close(fd);
}

static void data_set_id(data_t *root) {
    sprintf(root->datafile, "%s/rkv-data-%04u", root->datadir, root->dataid);
}

static int data_open_id(data_t *root, uint16_t id) {
    char temp[PATH_MAX];
    int fd;

    sprintf(temp, "%s/rkv-data-%04u", root->datadir, id);

    if((fd = open(temp, O_RDONLY, 0600)) < 0)
        diep(temp);

    return fd;
}

static void data_open_final(data_t *root) {
    if((root->datafd = open(root->datafile, O_CREAT | O_RDWR | O_APPEND, 0600)) < 0)
        diep(root->datafile);

    // skipping header
    lseek(root->datafd, 0, SEEK_END);

    printf("[+] active data file: %s\n", root->datafile);
}

size_t data_jump_next() {
    printf("[+] jumping to the next data file\n");

    // closing current file descriptor
    close(rootdata->datafd);

    // moving to the next file
    rootdata->dataid += 1;
    data_set_id(rootdata);

    data_initialize(rootdata->datafile);
    data_open_final(rootdata);

    return rootdata->dataid;
}

char *data_get(size_t offset, size_t length, uint16_t dataid) {
    char *buffer = malloc(length);
    int fd = rootdata->datafd;

    if(rootdata->dataid != dataid) {
        printf("not on same data id, %d <> %d\n", rootdata->dataid, dataid);
        fd = data_open_id(rootdata, dataid);
    }

    lseek(fd, offset + sizeof(data_header_t), SEEK_SET);
    if(read(fd, buffer, length) != (ssize_t) length) {
        warnp("data_get: read");

        free(buffer);
        buffer = NULL;

        goto cleanup;
    }

cleanup:
    if(rootdata->dataid != dataid) {
        close(fd);
    }

    return buffer;
}

size_t data_insert(char *data, unsigned char *hash, uint32_t length) {
    size_t offset = lseek(rootdata->datafd, 0, SEEK_CUR);
    data_header_t header;

    memcpy(header.hash, hash, HASHSIZE);
    header.length = length;

    // data offset will always be >= 1
    // we can use 0 as error detection

    if(write(rootdata->datafd, &header, sizeof(data_header_t)) != sizeof(data_header_t)) {
        fprintf(stderr, "[-] cannot write data header\n");
        return 0;
    }

    if(write(rootdata->datafd, data, length) != (ssize_t) length) {
        fprintf(stderr, "[-] cannot write data\n");
        return 0;
    }

    return offset;
}

//
// data constructor and destructor
//
void data_destroy() {
    free(rootdata->datafile);
    free(rootdata);
}

void data_init(uint16_t dataid) {
    data_t *lroot = (data_t *) malloc(sizeof(data_t));

    lroot->datadir = "/mnt/storage/tmp/rkv";
    lroot->datafile = malloc(sizeof(char) * (PATH_MAX + 1));
    lroot->dataid = dataid;

    data_set_id(lroot);

    // opening the file and creating it if needed
    data_initialize(lroot->datafile);

    // opening the final file for appending only
    data_open_final(lroot);

    // commit variable
    rootdata = lroot;
}

