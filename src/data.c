#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <x86intrin.h>
#include "zerodb.h"
#include "data.h"

static data_t *rootdata = NULL;

//
// data management
//
void data_initialize(char *filename) {
    int fd;

    if((fd = open(filename, O_CREAT | O_RDWR, 0600)) < 0)
        diep(filename);

    // writing initial header
    // this header is, right now, not used and not useful
    // we should probably improve it
    //
    // anyway, it's important for the implementation to have
    // at least 1 byte already written, we use 0 as error when
    // expecting an offset
    if(write(fd, "X", 1) != 1)
        diep(filename);

    close(fd);
}

// simply set globaly the current filename based on it's id
static void data_set_id(data_t *root) {
    sprintf(root->datafile, "%s/rkv-data-%04u", root->datadir, root->dataid);
}

// open the datafile based on it's id
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

// jumping to the next id close the current data file
// and open the next id file, it will create the new file
size_t data_jump_next() {
    verbose("[+] jumping to the next data file\n");

    // closing current file descriptor
    close(rootdata->datafd);

    // moving to the next file
    rootdata->dataid += 1;
    data_set_id(rootdata);

    data_initialize(rootdata->datafile);
    data_open_final(rootdata);

    return rootdata->dataid;
}

static uint32_t data_crc32(const uint8_t *bytes, ssize_t length) {
    uint64_t *input = (uint64_t *) bytes;
    uint32_t hash = 0;
    ssize_t i = 0;

    for(i = 0; i < length - 8; i += 8)
        hash = _mm_crc32_u64(hash, *input++);

    for(; i < length; i++)
        hash = _mm_crc32_u8(hash, bytes[i]);

    return hash;
}

// get a payload from any datafile
unsigned char *data_get(size_t offset, size_t length, uint16_t dataid, uint8_t idlength) {
    unsigned char *buffer = malloc(length);
    int fd = rootdata->datafd;

    if(rootdata->dataid != dataid) {
        // the requested datafile is not the current datafile opened
        // we will re-open the expected datafile temporary

        // printf("[-] current data file: %d, requested: %d, switching\n", rootdata->dataid, dataid);
        fd = data_open_id(rootdata, dataid);
    }

    lseek(fd, offset + sizeof(data_header_t) + idlength, SEEK_SET);
    if(read(fd, buffer, length) != (ssize_t) length) {
        warnp("data_get: read");

        free(buffer);
        buffer = NULL;
    }

    if(rootdata->dataid != dataid) {
        // closing the temporary file descriptor
        // we only keep the current one
        close(fd);
    }

    return buffer;
}

// insert data on the datafile and returns it's offset
size_t data_insert(unsigned char *data, uint32_t datalength, unsigned char *id, uint8_t idlength) {
    size_t offset = lseek(rootdata->datafd, 0, SEEK_CUR);
    size_t headerlength = sizeof(data_header_t) + idlength;
    data_header_t *header;

    if(!(header = malloc(headerlength)))
        diep("malloc");

    header->idlength = idlength;
    header->datalength = datalength;
    header->integrity = data_crc32(data, datalength);

    memcpy(header->id, id, idlength);

    // data offset will always be >= 1
    // we can use 0 as error detection

    if(write(rootdata->datafd, header, headerlength) != (ssize_t) headerlength) {
        fprintf(stderr, "[-] cannot write data header\n");
        free(header);
        return 0;
    }

    free(header);

    if(write(rootdata->datafd, data, datalength) != (ssize_t) datalength) {
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

void data_init(uint16_t dataid, char *datapath) {
    data_t *lroot = (data_t *) malloc(sizeof(data_t));

    lroot->datadir = datapath;
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

void data_emergency() {
    fsync(rootdata->datafd);
}
