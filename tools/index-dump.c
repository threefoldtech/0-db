#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <x86intrin.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <time.h>
#include "zerodb.h"
#include "index.h"

void *warnp(char *str) {
    fprintf(stderr, "[-] %s: %s\n", str, strerror(errno));
    return NULL;
}

void diep(char *str) {
    warnp(str);
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

static char *index_date(uint32_t epoch, char *target, size_t length) {
    struct tm *timeval;
    time_t unixtime;

    unixtime = epoch;

    timeval = localtime(&unixtime);
    strftime(target, length, "%F %T", timeval);

    return target;
}

int index_dump(int fd) {
    index_t header;
    char entrydate[64];

    // first step, let's validate the header
    if(read(fd, &header, sizeof(index_t)) != (size_t) sizeof(index_t)) {
        fprintf(stderr, "[-] cannot read index header\n");
        return 1;
    }

    if(memcmp(header.magic, "IDX0", 4) != 0) {
        fprintf(stderr, "[-] index header magic mismatch\n");
        return 1;
    }

    printf("[+] index header seems correct\n");

    // now it's time to read each entries
    // each time, one entry starts by the entry-header
    // then entry payload.
    // the entry headers starts with the amount of bytes
    // of the key, which is needed to read the full header
    uint8_t idlength;
    index_item_t *entry = NULL;
    size_t entrycount = 0;

    while(read(fd, &idlength, sizeof(idlength)) == sizeof(idlength)) {
        // we have the length of the key
        ssize_t entrylength = sizeof(index_item_t) + idlength;
        if(!(entry = realloc(entry, entrylength)))
            diep("realloc");

        // rollback the 1 byte read for the id length
        off_t curoff = lseek(fd, -1, SEEK_CUR);

        if(read(fd, entry, entrylength) != entrylength)
            diep("index header read failed");

        entrycount += 1;

        index_date(entry->timestamp, entrydate, sizeof(entrydate));

        printf("[+] index entry: %lu, offset: %lu\n", entrycount, curoff);
        printf("[+]   id length  : %d\n", entry->idlength);
        printf("[+]   data length: %" PRIu64 "\n", entry->length);
        printf("[+]   data offset: %" PRIu64 "\n", entry->offset);
        printf("[+]   data fileid: %u\n", entry->dataid);
        printf("[+]   entry flags: 0x%X\n", entry->flags);
        printf("[+]   entry date : %s\n", entrydate);
        printf("[+]   entry key  : ");
        hexdump(entry->id, entry->idlength);
        printf("\n");
    }

    printf("[+] ---------------------------\n");
    printf("[+] all done, entry found: %lu\n", entrycount);

    free(entry);

    return 0;
}

int main(int argc, char *argv[]) {
    char *filename = NULL;
    int fd;
    struct stat sb;

    if(argc < 2) {
        fprintf(stderr, "Usage: %s index-filename\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    filename = argv[1];
    printf("[+] dumping index: %s\n", filename);

    if(stat(filename, &sb) != 0)
        diep(filename);

    if(S_ISDIR(sb.st_mode)) {
        fprintf(stderr, "[-] %s: target is a directory\n", filename);
        exit(EXIT_FAILURE);
    }

    if((fd = open(filename, O_RDONLY, 0600)) < 0)
        diep(filename);

    index_dump(fd);
    close(fd);

    return 0;
}
