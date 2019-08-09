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
#ifdef SHADUMP
#include <openssl/sha.h>
#endif
#include "libzdb.h"
#include "data.h"

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

#ifdef SHADUMP
void sha256dump(char *input, size_t length) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;

    SHA256_Init(&sha256);
    SHA256_Update(&sha256, input, length);
    SHA256_Final(hash, &sha256);

    hexdump(hash, SHA256_DIGEST_LENGTH);
}
#endif

uint32_t data_crc32(const uint8_t *bytes, ssize_t length) {
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
    data_header_t header;
    int errors = 0;

    // first step, let's validate the header
    if(read(fd, &header, sizeof(data_header_t)) != (size_t) sizeof(data_header_t)) {
        fprintf(stderr, "[-] cannot read data header\n");
        return 1;
    }

    if(memcmp(header.magic, "DAT0", 4) != 0) {
        fprintf(stderr, "[-] header magic mismatch\n");
        return 1;
    }

    if(header.version != ZDB_DATAFILE_VERSION) {
        fprintf(stderr, "[-] version mismatch (%d <> %d)\n", header.version, ZDB_DATAFILE_VERSION);
        return 1;
    }

    printf("[+] data header seems correct\n");

    // now it's time to read each entries
    // each time, one entry starts by the entry-header
    // then entry payload.
    // the entry headers starts with the amount of bytes
    // of the key, which is needed to read the full header
    uint8_t idlength;
    data_entry_header_t *entry = NULL;
    size_t entrycount = 0;
    char *buffer = NULL;

    while(read(fd, &idlength, sizeof(idlength)) == sizeof(idlength)) {
        // we have the length of the key
        ssize_t entrylength = sizeof(data_entry_header_t) + idlength;
        if(!(entry = realloc(entry, entrylength)))
            diep("realloc");

        // rollback the 1 byte read for the id length
        off_t current = lseek(fd, -1, SEEK_CUR);

        if(read(fd, entry, entrylength) != entrylength)
            diep("data header read failed");

        entrycount += 1;

        printf("[+] data entry: %lu, id length: %d\n", entrycount, entry->idlength);
        printf("[+]   expected length: %u, current offset: %ld\n", entry->datalength, current);
        printf("[+]   previous offset: %u\n", entry->previous);
        printf("[+]   expected crc: %08x\n", entry->integrity);
        printf("[+]   entry key: ");
        hexdump(entry->id, entry->idlength);
        printf("\n");

        if(entry->datalength == 0)
            continue;

        if(!(buffer = realloc(buffer, entry->datalength)))
            diep("realloc");

        if(read(fd, buffer, entry->datalength) != entry->datalength)
            diep("payload entry read failed");

        uint32_t crc = data_crc32((uint8_t *) buffer, entry->datalength);

        if(crc != entry->integrity) {
            fprintf(stderr, "[-] integrity check failed: %08x <> %08x\n", crc, entry->integrity);
            errors += 1;

        } else {
            printf("[+]   data crc: match\n");
        }

        #ifdef SHADUMP
        printf("[+]   data sha256: ");
        sha256dump(buffer, entry->datalength);
        printf("\n");
        #endif
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
