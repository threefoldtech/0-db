#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <x86intrin.h>
#include <sys/stat.h>
#include "data.h"

void warnp(char *str) {
    fprintf(stderr, "[-] %s: %s\n", str, strerror(errno));
}

void diep(char *str) {
    warnp(str);
    exit(EXIT_FAILURE);
}

static uint32_t data_crc32(const char *bytes, ssize_t length) {
    uint64_t *input = (uint64_t *) bytes;
    uint32_t hash = 0;
    ssize_t i = 0;

    for(i = 0; i < length - 8; i += 8)
        hash = _mm_crc32_u64(hash, *input++);

    for(; i < length; i++)
        hash = _mm_crc32_u8(hash, bytes[i]);

    return hash;
}

int data_integrity(int fd) {
    char *buffer = NULL;
    int errors = 0;

    // first step, let's validate the header
    // for now we use a dumb header of 1 byte which should
    // contains 'X' (this will probably be improved later)
    if(!(buffer = calloc(sizeof(char), 1)))
        diep("malloc");

    if(read(fd, buffer, 1) != 1) {
        fprintf(stderr, "[-] cannot read data header\n");
        return 1;
    }

    if(*buffer != 'X') {
        fprintf(stderr, "[-] header file mismatch\n");
        return 1;
    }

    printf("[+] data header correct\n");

    // now it's time to read each entries
    // each time, one entry starts by the entry-header
    // then entry payload.
    // the entry headers starts with the amount of bytes
    // of the key, which is needed to read the full header
    uint8_t idlength;
    data_header_t *entry = NULL;
    size_t entrycount = 0;

    while(read(fd, &idlength, sizeof(idlength)) == sizeof(idlength)) {
        // we have the length of the key
        ssize_t entrylength = sizeof(data_header_t) + idlength;
        if(!(entry = realloc(entry, entrylength)))
            diep("realloc");

        // rollback the 1 byte read for the id length
        off_t current = lseek(fd, -1, SEEK_CUR);

        if(read(fd, entry, entrylength) != entrylength)
            diep("data header read failed");

        entrycount += 1;

        printf("[+] data entry: %lu, id length: %d\n", entrycount, entry->idlength);
        printf("[+]   expected length: %u, current offset: %ld\n", entry->datalength, current);
        printf("[+]   payload crc: %08x\n", entry->integrity);
        printf("[+]   entry key: <%.*s>\n", entry->idlength, entry->id);

        if(!(buffer = realloc(buffer, entry->datalength)))
            diep("realloc");

        if(read(fd, buffer, entry->datalength) != entry->datalength)
            diep("payload entry read failed");

        uint32_t crc = data_crc32(buffer, entry->datalength);

        if(crc != entry->integrity) {
            fprintf(stderr, "[-] integrity check failed: %08x <> %08x\n", crc, entry->integrity);
            errors += 1;

        } else {
            printf("[+]   data crc: match\n");
        }
    }

    free(entry);



    return errors;
}

int main(int argc, char *argv[]) {
    char *filename = NULL;
    int val = 0, fd;
    struct stat sb;

    if(argc < 2) {
        fprintf(stderr, "Usage: %s data-filename\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    filename = argv[1];
    printf("[+] checking data integrity of: %s\n", filename);

    if(stat(filename, &sb) != 0)
        diep(filename);

    if(S_ISDIR(sb.st_mode)) {
        fprintf(stderr, "[-] %s: target is a directory\n", filename);
        exit(EXIT_FAILURE);
    }

    if((fd = open(filename, O_RDONLY, 0600)) < 0)
        diep(filename);

    if((val = data_integrity(fd)))
        fprintf(stderr, "[-] data file inconsistency: %d error(s)\n", val);

    close(fd);

    return val;
}
