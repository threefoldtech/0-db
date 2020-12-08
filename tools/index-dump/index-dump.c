#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include "libzdb.h"

int index_dump_files(index_root_t *zdbindex, uint64_t maxfile) {
    index_header_t *header;
    char datestr[64];
    size_t totalentries = 0;

    for(uint16_t fileid = 0; fileid < maxfile; fileid += 1) {
        //
        // loading index file
        //
        zdb_index_open_readonly(zdbindex, fileid);

        if(!(header = zdb_index_descriptor_load(zdbindex)))
            return 1;

        if(!zdb_index_descriptor_validate(header, zdbindex))
            return 1;

        printf("[+] index-dump: header seems correct\n");
        printf("[+] index-dump: created at: %s\n", zdb_header_date(header->created, datestr, sizeof(datestr)));
        printf("[+] index-dump: last open: %s\n", zdb_header_date(header->opened, datestr, sizeof(datestr)));
        printf("[+] index-dump: index mode: %s\n", zdb_running_mode(header->mode));

        //
        // dumping contents
        //
        index_item_t *entry = NULL;
        size_t entrycount = 0;
        off_t curoff;

        curoff = zdb_index_raw_offset(zdbindex);

        while((entry = zdb_index_raw_fetch_entry(zdbindex))) {
            entrycount += 1;
            totalentries += 1;

            zdb_header_date(entry->timestamp, datestr, sizeof(datestr));

            printf("[+] index entry: %lu, offset: %" PRId64 "\n", entrycount, (int64_t) curoff);
            printf("[+]   id length  : %" PRIu8 "\n", entry->idlength);
            printf("[+]   data length: %" PRIu32 "\n", entry->length);
            printf("[+]   data offset: %" PRIu32 "\n", entry->offset);
            printf("[+]   data fileid: %" PRIu16 "\n", entry->dataid);
            printf("[+]   entry flags: 0x%X\n", entry->flags);
            printf("[+]   entry date : %s\n", datestr);
            printf("[+]   previous   : %" PRIu32 "\n", entry->previous);
            printf("[+]   data crc   : %08x\n", entry->crc);
            printf("[+]   parent id  : %" PRIu16 "\n", entry->parentid);
            printf("[+]   parent offs: %" PRIu32 "\n", entry->parentoff);
            printf("[+]   entry key  : ");
            zdb_tools_hexdump(entry->id, entry->idlength);
            printf("\n");

            // saving current offset
            curoff = zdb_index_raw_offset(zdbindex);
            free(entry);
        }

        printf("[+] ---------------------------\n");
        printf("[+] file done, file entries found: %lu\n", entrycount);

        zdb_index_close(zdbindex);
    }

    printf("[+] ---------------------------\n");
    printf("[+] all done, entries found: %lu\n", totalentries);

    return 0;
}

int main(int argc, char *argv[]) {
    char *dirname = NULL;

    if(argc < 2) {
        fprintf(stderr, "Usage: %s index-path\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    // loading zdb
    printf("[*] 0-db engine v%s\n", zdb_version());

    // fetching directory path
    dirname = argv[1];
    printf("[+] index-dump: loading: %s\n", dirname);

    if(zdb_dir_exists(dirname) != ZDB_DIRECTORY_EXISTS) {
        fprintf(stderr, "[-] index-dump: could not reach index directory\n");
        exit(EXIT_FAILURE);
    }

    // initializing database
    zdb_settings_t *zdb_settings = zdb_initialize();
    zdb_id_set("index-dump");

    // WARNING: we *don't* open the database, this would populate
    //          memory, reading and loading everything... we just want
    //          to initialize structs but not loads anything
    //
    //          we will shortcut the database loading and directly
    //          call the low-level index lazy loader (lazy loader won't
    //          load anything except structs)
    //
    // zdb_open(zdb_settings);
    index_root_t *zdbindex;

    if(!(zdbindex = zdb_index_init_lazy(zdb_settings, dirname, NULL))) {
        fprintf(stderr, "[-] index-dump: cannot load index\n");
        exit(EXIT_FAILURE);
    }

    uint64_t maxfile = zdb_index_availity_check(zdbindex);
    if(maxfile == 0) {
        fprintf(stderr, "[-] index-dump: no index files found\n");
        exit(EXIT_FAILURE);
    }

    // dumping index
    return index_dump_files(zdbindex, maxfile);
}
