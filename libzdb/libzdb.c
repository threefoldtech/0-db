#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <signal.h>
#include <getopt.h>
#include <ctype.h>
#include <sys/time.h>
#include "libzdb.h"
#include "libzdb_private.h"

//
// global system settings
//
zdb_settings_t zdb_rootsettings = {
    .datapath = ZDB_DEFAULT_DATAPATH,
    .indexpath = ZDB_DEFAULT_INDEXPATH,
    .verbose = 0,
    .dump = 0,
    .sync = 0,
    .synctime = 0,
    .mode = ZDB_MODE_KEY_VALUE,
    .hook = NULL,
    .datasize = ZDB_DEFAULT_DATA_MAXSIZE,
    .maxsize = 0,
    .initialized = 0,
};


// debug tools
static char __hex[] = "0123456789abcdef";

void zdb_fulldump(void *_data, size_t len) {
    uint8_t *data = _data;
    unsigned int i, j;

    printf("[*] data fulldump [%p -> %p] (%lu bytes)\n", data, data + len, len);
    printf("[*] 0x0000: ");

    for(i = 0; i < len; ) {
        printf("%02x ", data[i++]);

        if(i % 16 == 0) {
            printf("|");

            for(j = i - 16; j < i; j++)
                printf("%c", ((isprint(data[j]) ? data[j] : '.')));

            printf("|\n[*] 0x%04x: ", i);
        }
    }

    if(i % 16) {
        printf("%-*s |", 5 * (16 - (i % 16)), " ");

        for(j = i - (i % 16); j < len; j++)
            printf("%c", ((isprint(data[j]) ? data[j] : '.')));

        printf("%-*s|\n", 16 - ((int) len % 16), " ");
    }

    printf("\n");
}

// public wrapper
void zdb_tools_fulldump(void *_data, size_t len) {
    return zdb_fulldump(_data, len);
}

void zdb_hexdump(void *input, size_t length) {
    unsigned char *buffer = (unsigned char *) input;
    char *output = calloc((length * 2) + 1, 1);
    char *writer = output;

    for(unsigned int i = 0, j = 0; i < length; i++, j += 2) {
        *writer++ = __hex[(buffer[i] & 0xF0) >> 4];
        *writer++ = __hex[buffer[i] & 0x0F];
    }

    printf("0x%s\n", output);
    free(output);
}

// public wrapper
void zdb_tools_hexdump(void *input, size_t length) {
    return zdb_hexdump(input, length);
}

//
// global warning and fatal message
//
void *zdb_warnp(char *str) {
    zdb_timelog(stderr);
    fprintf(stderr, COLOR_YELLOW "[-] %s: %s" COLOR_RESET "\n", str, strerror(errno));
    return NULL;
}

void zdb_verbosep(char *prefix, char *str) {
#ifdef RELEASE
    // only match on verbose flag if we are
    // in release mode, otherwise do always the
    // print, we are in debug mode anyway
    if(!zdb_rootsettings.verbose)
        return;
#endif

    zdb_timelog(stderr);
    fprintf(stderr, "[-] %s: %s: %s\n", prefix, str, strerror(errno));
}

void zdb_diep(char *str) {
    zdb_warnp(str);
    exit(EXIT_FAILURE);
}

void zdb_timelog(FILE *fp) {
    struct timeval n, *b;
    double value = 0;

    // boot time
    b = &zdb_rootsettings.stats.inittime;

    gettimeofday(&n, NULL);
    value = (double)(n.tv_usec - b->tv_usec) / 1000000 + (double)(n.tv_sec - b->tv_sec);

    fprintf(fp, "[% 15.6f]", value);
}

char *zdb_header_date(uint32_t epoch, char *target, size_t length) {
    struct tm *timeval;
    time_t unixtime;

    unixtime = epoch;

    timeval = localtime(&unixtime);
    strftime(target, length, "%F %T", timeval);

    return target;
}

