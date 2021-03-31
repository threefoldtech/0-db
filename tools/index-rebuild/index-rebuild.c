#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>
#include <getopt.h>
#include <unistd.h>
#include "libzdb.h"

static struct option long_options[] = {
    {"data",       required_argument, 0, 'd'},
    {"index",      required_argument, 0, 'i'},
    {"namespace",  required_argument, 0, 'n'},
    {"template",   required_argument, 0, 't'},
    {"mode",       required_argument, 0, 'm'},
    {"time",       required_argument, 0, 'T'},
    {"help",       no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

int index_data_jump_to(fileid_t fileid, index_root_t *zdbindex, data_root_t *zdbdata) {
    data_header_t *header;
    char datestr[64];

    // closing previous data fd
    if(zdbdata->datafd)
        close(zdbdata->datafd);

    printf("[+] index-rebuild: opening datafile id: %d\n", fileid);

    // opening data file
    zdbdata->dataid = fileid;
    if((zdbdata->datafd = zdb_data_open_readonly(zdbdata)) < 0)
        return 1;

    // validating header
    if(!(header = zdb_data_descriptor_load(zdbdata)))
        return 1;

    if(!zdb_data_descriptor_validate(header, zdbdata))
        return 1;

    printf("[+] index-rebuild: data header seems correct\n");
    printf("[+] index-rebuild: data created at: %s\n", zdb_header_date(header->created, datestr, sizeof(datestr)));
    printf("[+] index-rebuild: data last open: %s\n", zdb_header_date(header->opened, datestr, sizeof(datestr)));

    // only take care of next index id if it's not the first
    // doing it at the end to avoid creating an empty index
    // for non-existing data
    if(fileid > 0)
        index_jump_next(zdbindex);

    return 0;
}

ssize_t index_rebuild_pass(index_root_t *zdbindex, data_root_t *zdbdata, time_t timestamp) {
    size_t entrycount = 0;
    uint8_t idlength;
    data_entry_header_t *entry = NULL;

    // now it's time to read each entries
    // each time, one entry starts by the entry-header
    // then entry payload.
    // the entry headers starts with the amount of bytes
    // of the key, which is needed to read the full header

    while(read(zdbdata->datafd, &idlength, sizeof(idlength)) == sizeof(idlength)) {
        // we have the length of the key
        ssize_t entrylength = sizeof(data_entry_header_t) + idlength;
        if(!(entry = realloc(entry, entrylength)))
            zdb_diep("realloc");

        // rollback the 1 byte read for the id length
        off_t current = lseek(zdbdata->datafd, -1, SEEK_CUR);

        if(read(zdbdata->datafd, entry, entrylength) != entrylength)
            zdb_diep("data header read failed");

        printf("[+] processing key: ");
        zdb_tools_hexdump(entry->id, entry->idlength);

        if(timestamp > 0 && entry->timestamp > timestamp) {
            printf("[+] index-rebuild: timestamp limit reached, stopping here\n");
            return entrycount;
        }

        index_entry_t idxreq = {
            .idlength = entry->idlength,
            .offset = current,
            .length = entry->datalength,
            .flags = entry->flags,
            .dataid = zdbdata->dataid,
            .indexid = zdbindex->indexid,
            .crc = entry->integrity,
            .timestamp = entry->timestamp,
        };

        index_set_t setter = {
            .entry = &idxreq,
            .id = entry->id,
        };

        // fetching existing if any
        // this is needed to keep track of the history
        index_entry_t *existing = index_get(zdbindex, entry->id, entry->idlength);

        if(entry->flags & DATA_ENTRY_DELETED) {
            if(index_entry_delete(zdbindex, existing)) {
                fprintf(stderr, "[-] index-rebuild: could not delete index item\n");
                return -1;
            }

        } else {
            if(!index_set(zdbindex, &setter, existing)) {
                fprintf(stderr, "[-] index-rebuild: could not insert index item\n");
                return -1;
            }
        }

        // skipping data payload
        lseek(zdbdata->datafd, entry->datalength, SEEK_CUR);
        entrycount += 1;
    }

    free(entry);

    printf("[+] index-rebuild: index pass entries: %lu\n", entrycount);

    return entrycount;
}


int index_rebuild(index_root_t *zdbindex, data_root_t *zdbdata, time_t timestamp) {
    fileid_t fileid = 0;
    ssize_t entries = 0;
    size_t entrycount = 0;

    // prorcessing all files
    for(fileid = 0; ; fileid += 1) {
        // setting index and data id to new id
        if(index_data_jump_to(fileid, zdbindex, zdbdata))
            break;

        // processing this file
        if((entries = index_rebuild_pass(zdbindex, zdbdata, timestamp)) < 0)
            break;

        entrycount += entries;
    }

    zdb_success("[+] index rebuilt (%lu entries inserted)", entrycount);

    return 0;
}

void usage() {
    printf("Index rebuild tool arguments:\n\n");

    printf("  --data      <dir>      datafile directory (root path), input\n");
    printf("  --index     <dir>      indexfile directory (root path), output\n");
    printf("  --namespace <name>     which namespace to compact\n");
    printf("  --template  <file>     zdb-namespace source file (namespace settings)\n");
    printf("  --mode      <mode>     zdb mode used ('user' or 'seq' expected)\n");
    printf("  --time      <timest>   rebuild up to that timestamp (rollback in time)\n");
    printf("  --help                 print this message\n");

    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int option_index = 0;
    char *indexpath = NULL;
    char *datapath = NULL;
    char *nsname = NULL;
    char *template = NULL;
    time_t timestamp = 0;
    int mode = -1;

    while(1) {
        // int i = getopt_long_only(argc, argv, "d:i:l:p:vxh", long_options, &option_index);
        int i = getopt_long_only(argc, argv, "", long_options, &option_index);

        if(i == -1)
            break;

        switch(i) {
            case 'd':
                datapath = optarg;
                break;

            case 'i':
                indexpath = optarg;
                break;

            case 'n':
                nsname = optarg;
                break;

            case 't':
                template = optarg;
                break;

            case 'T':
                timestamp = atoi(optarg);
                break;

            case 'm':
                if(strcmp(optarg, "user") == 0) {
                    mode = ZDB_MODE_KEY_VALUE;

                } else if(strcmp(optarg, "seq") == 0) {
                    mode = ZDB_MODE_SEQUENTIAL;

                } else {
                    fprintf(stderr, "[-] invalid mode: 'user' or 'seq' expected\n");
                    exit(EXIT_FAILURE);
                }

                break;

            case 'h':
                usage();
                break;

            case '?':
            default:
                fprintf(stderr, "Unsupported option\n");
                exit(EXIT_FAILURE);
        }
    }

    if(mode == -1) {
        fprintf(stderr, "[-] missing mode (user or sequential)\n");
        usage();
    }

    if(!indexpath || !datapath) {
        fprintf(stderr, "[-] missing index or data directory\n");
        usage();
    }

    if(!nsname) {
        fprintf(stderr, "[-] missing namespace name, you need to specify a namespace\n");
        usage();
    }

    // loading zdb
    printf("[*] 0-db engine v%s\n", zdb_version());
    printf("[+] template: %s\n", template ? template : "not set");

    // fetching directory path
    if(zdb_dir_exists(datapath) != ZDB_DIRECTORY_EXISTS) {
        fprintf(stderr, "[-] index-rebuild: could not reach data directory\n");
        exit(EXIT_FAILURE);
    }

    // initializing database
    // and setting target mode
    zdb_settings_t *zdb_settings = zdb_initialize();
    zdb_settings->mode = mode;

    // WARNING: if the correct mode is not set
    // rebuild will lead to incorrect consistancy
    // with data and is undefined behavoior

    zdb_id_set("index-rebuild");

    zdb_settings->indexpath = indexpath;
    zdb_settings->datapath = datapath;

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
    data_root_t *zdbdata;

    ns_root_t *nsroot = namespaces_allocate(zdb_settings);
    namespace_t *namespace;

    if(!(namespace = namespace_load_light(nsroot, nsname, 1))) {
        fprintf(stderr, "[-] index-rebuild: could not initialize namespace\n");
        exit(EXIT_FAILURE);
    }

    if(!(zdbdata = zdb_data_init_lazy(zdb_settings, namespace->datapath, 0))) {
        fprintf(stderr, "[-] index-rebuild: cannot load data files\n");
        exit(EXIT_FAILURE);
    }

    if(!(zdbindex = zdb_index_init(zdb_settings, namespace->indexpath, namespace, nsroot->branches))) {
        fprintf(stderr, "[-] index-rebuild: cannot initialize index\n");
        exit(EXIT_FAILURE);
    }

    if(timestamp > 0) {
        printf("[+] index-rebuild: only rebuild up-to: %ld\n", timestamp);
    }

    return index_rebuild(zdbindex, zdbdata, timestamp);
}
