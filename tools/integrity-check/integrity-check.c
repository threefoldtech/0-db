#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <libgen.h>
#include <inttypes.h>
#ifdef SHADUMP
#include <openssl/sha.h>
#endif
#include "libzdb.h"
#include "data.h"

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

int data_integrity(data_root_t *zdbdata) {
    data_header_t *header;
    char datestr[64];
    int errors = 0;

    // opening data file
    zdbdata->datafd = zdb_data_open_readonly(zdbdata);

    // validating header
    if(!(header = zdb_data_descriptor_load(zdbdata)))
        return 1;

    if(!zdb_data_descriptor_validate(header, zdbdata))
        return 1;

    printf("[+] integrity-check: header seems correct\n");
    printf("[+] integrity-check: created at: %s\n", zdb_header_date(header->created, datestr, sizeof(datestr)));
    printf("[+] integrity-check: last open: %s\n", zdb_header_date(header->opened, datestr, sizeof(datestr)));

    // now it's time to read each entries
    // each time, one entry starts by the entry-header
    // then entry payload.
    // the entry headers starts with the amount of bytes
    // of the key, which is needed to read the full header
    uint8_t idlength;
    data_entry_header_t *entry = NULL;
    size_t entrycount = 0;
    char *buffer = NULL;

    while(read(zdbdata->datafd, &idlength, sizeof(idlength)) == sizeof(idlength)) {
        // we have the length of the key
        ssize_t entrylength = sizeof(data_entry_header_t) + idlength;
        if(!(entry = realloc(entry, entrylength)))
            zdb_diep("realloc");

        // rollback the 1 byte read for the id length
        off_t current = lseek(zdbdata->datafd, -1, SEEK_CUR);

        if(read(zdbdata->datafd, entry, entrylength) != entrylength)
            zdb_diep("data header read failed");

        entrycount += 1;

        printf("[+] data entry: %lu, id length: %d\n", entrycount, entry->idlength);
        printf("[+]   expected length: %u, current offset: %" PRId64 "\n", entry->datalength, (int64_t) current);
        printf("[+]   previous offset: %u\n", entry->previous);
        printf("[+]   expected crc: %08x\n", entry->integrity);
        printf("[+]   entry key: ");
        zdb_tools_hexdump(entry->id, entry->idlength);
        printf("\n");

        if(entry->datalength == 0)
            continue;

        if(!(buffer = realloc(buffer, entry->datalength)))
            zdb_diep("realloc");

        if(read(zdbdata->datafd, buffer, entry->datalength) != entry->datalength)
            zdb_diep("payload entry read failed");

        uint32_t crc = zdb_checksum_crc32((uint8_t *) buffer, entry->datalength);

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
    if(argc < 2) {
        fprintf(stderr, "Usage: %s data-filename\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char *filename = argv[1];
    printf("[+] checking data integrity of: %s\n", filename);

    if(zdb_file_exists(filename) != ZDB_FILE_EXISTS) {
        fprintf(stderr, "[-] %s: invalid target file\n", filename);
        exit(EXIT_FAILURE);
    }

    // splitting directory and file from argument
    char *filedir = dirname(strdup(filename));
    char *datafile = basename(filename);

    if(strncmp(datafile, "zdb-data-", 9)) {
        fprintf(stderr, "[-] integrity-check: data filename seems wrong\n");
        exit(EXIT_FAILURE);
    }

    uint16_t dataid = atoi(datafile + 9);

    printf("[+] datafile path: %s\n", filedir);
    printf("[+] datafile name: %s\n", datafile);
    printf("[+] datafile id  : %d\n", dataid);

    zdb_settings_t *zdb_settings = zdb_initialize();
    zdb_id_set("integrity-checker");

    // WARNING: we *don't* open the database, this would populate
    //          memory, reading and loading everything... we just want
    //          to initialize structs but not loads anything
    //
    //          we will shortcut the database loading and directly
    //          call the low-level data lazy loader (lazy loader won't
    //          load anything except structs)
    //
    // zdb_open(zdb_settings);
    data_root_t *zdbdata;

    if(!(zdbdata = zdb_data_init_lazy(zdb_settings, filedir, dataid))) {
        fprintf(stderr, "[-] integrity-check: cannot load data\n");
        exit(EXIT_FAILURE);
    }

    int val = 0;

    if((val = data_integrity(zdbdata)))
        fprintf(stderr, "[-] data file inconsistency: %d error(s)\n", val);

    return val;
}

