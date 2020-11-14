#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>
#include <getopt.h>
#include <unistd.h>
#include "libzdb.h"

static struct option long_options[] = {
    {"in-data",     required_argument, 0, 'd'},
    {"in-index",    required_argument, 0, 'i'},
    {"out-data",    required_argument, 0, 'D'},
    {"out-index",   required_argument, 0, 'I'},
    {"namespace",   required_argument, 0, 'n'},
    {"help",        no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

typedef struct instance_t {
    zdb_settings_t *zdbsettings;

    char *indexpath;
    index_root_t *zdbindex;

    char *datapath;
    data_root_t *zdbdata;

    ns_root_t *nsroot;
    namespace_t *namespace;

} instance_t;

int index_data_jump_to(uint16_t fileid, index_root_t *zdbindex, data_root_t *zdbdata) {
    data_header_t *header;
    char datestr[64];

    // only take care of next index id we it's not the first
    if(fileid > 0)
        index_jump_next(zdbindex);

    // closing previous data fd
    if(zdbdata->datafd)
        close(zdbdata->datafd);

    printf("[+] quick-compact: opening datafile id: %d\n", fileid);

    // opening data file
    zdbdata->dataid = fileid;
    if((zdbdata->datafd = zdb_data_open_readonly(zdbdata)) < 0)
        return 1;

    // validating header
    if(!(header = zdb_data_descriptor_load(zdbdata)))
        return 1;

    if(!zdb_data_descriptor_validate(header, zdbdata))
        return 1;

    printf("[+] quick-compact: data header seems correct\n");
    printf("[+] quick-compact: data created at: %s\n", zdb_header_date(header->created, datestr, sizeof(datestr)));
    printf("[+] quick-compact: data last open: %s\n", zdb_header_date(header->opened, datestr, sizeof(datestr)));

    return 0;
}

#if 0
size_t index_rebuild_pass(index_root_t *zdbindex, data_root_t *zdbdata) {
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

        entrycount += 1;

        printf("[+] processing key: ");
        zdb_tools_hexdump(entry->id, entry->idlength);
        printf("\n");

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

        if(!index_set(zdbindex, &setter, existing)) {
            fprintf(stderr, "[-] index-rebuild: could not insert index item\n");
            return 1;
        }

        // skipping data payload
        lseek(zdbdata->datafd, entry->datalength, SEEK_CUR);
    }

    free(entry);

    printf("[+] index-rebuild: index pass entries: %lu\n", entrycount);

    return entrycount;
}
#endif

int quick_compaction(instance_t *input, instance_t *output) {
    uint16_t fileid = 0;
    size_t entrycount = 0;

    // prorcessing all files
    for(fileid = 0; ; fileid += 1) {
        // setting index and data id to new id
        if(index_data_jump_to(fileid, input->zdbindex, input->zdbdata))
            break;

        // processing this file
        // entrycount += index_rebuild_pass(zdbindex, zdbdata);
    }

    zdb_success("[+] index rebuilt (%lu entries inserted)", entrycount);

    return 0;
}

void usage() {
    printf("Index rebuild tool arguments:\n\n");

    printf("  --in-data      <dir>      datafile directory (root path), input\n");
    printf("  --in-index     <dir>      indexfile directory (root path), input\n");
    printf("  --out-data     <dir>      datafile directory (root path), output\n");
    printf("  --out-index    <dir>      indexfile directory (root path), output\n");
    printf("  --namespace    <name>     which namespace to compact\n");
    printf("  --help                    print this message\n");

    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int option_index = 0;
    char *nsname = NULL;
    instance_t input;
    instance_t output;

    memset(&input, 0x00, sizeof(instance_t));
    memset(&output, 0x00, sizeof(instance_t));

    while(1) {
        // int i = getopt_long_only(argc, argv, "d:i:l:p:vxh", long_options, &option_index);
        int i = getopt_long_only(argc, argv, "", long_options, &option_index);

        if(i == -1)
            break;

        switch(i) {
            case 'd':
                input.datapath = optarg;
                break;

            case 'i':
                input.indexpath = optarg;
                break;

            case 'D':
                output.datapath = optarg;
                break;

            case 'I':
                output.indexpath = optarg;
                break;

            case 'n':
                nsname = optarg;
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

    if(!input.indexpath || !input.datapath) {
        fprintf(stderr, "[-] missing index or data input directory\n");
        usage();
    }

    if(!output.indexpath || !output.datapath) {
        fprintf(stderr, "[-] missing index or data output directory\n");
        usage();
    }

    if(!nsname) {
        fprintf(stderr, "[-] missing namespace name, you need to specify a namespace\n");
        usage();
    }

    printf("[+] input  index path: %s\n", input.indexpath);
    printf("[+] input  data  path: %s\n", input.datapath);
    printf("[+] output index path: %s\n", output.indexpath);
    printf("[+] output data  path: %s\n", output.datapath);

    // loading zdb
    printf("[*] 0-db engine v%s\n", zdb_version());

    // fetching directory path
    /*
    if(zdb_dir_exists(datapath) != ZDB_DIRECTORY_EXISTS) {
        fprintf(stderr, "[-] quick-compact: could not reach data directory\n");
        exit(EXIT_FAILURE);
    }
    */

    // initializing database
    // and setting target mode
    if(!(input.zdbsettings = zdb_initialize())) {
        fprintf(stderr, "[-] cannot initialize zdb input instance\n");
        exit(1);
    }

    input.zdbsettings->indexpath = input.indexpath;
    input.zdbsettings->datapath = input.datapath;

    // we can't open 2 database in the same time with current
    // zdb version, we won't open an output database but handle raw
    // files directly
    /*
    if(!(output.zdbsettings = zdb_initialize())) {
        fprintf(stderr, "[-] cannot initialize zdb output instance\n");
        exit(1);
    }
    output.zdbsettings->indexpath = output.indexpath;
    output.zdbsettings->datapath = output.datapath;
    */

    zdb_id_set("quick-compaction");

    // WARNING: we *don't* open the database, this would populate
    //          memory, reading and loading everything... we just want
    //          to initialize structs but not loads anything
    //
    //          we will shortcut the database loading and directly
    //          call the low-level index lazy loader (lazy loader won't
    //          load anything except structs)
    //
    // zdb_open(zdb_settings);

    //
    // input
    //
    input.nsroot = namespaces_allocate(input.zdbsettings);

    if(!(input.namespace = namespace_load_light(input.nsroot, nsname, 1))) {
        fprintf(stderr, "[-] quick-compact: intput: could not initialize namespace\n");
        exit(EXIT_FAILURE);
    }

    if(!(input.zdbdata = zdb_data_init_lazy(input.zdbsettings, input.namespace->datapath, 0))) {
        fprintf(stderr, "[-] quick-compact: input: cannot load data files\n");
        exit(EXIT_FAILURE);
    }

    if(!(input.zdbindex = zdb_index_init_lazy(input.zdbsettings, input.namespace->indexpath, input.namespace))) {
        fprintf(stderr, "[-] quick-compact: input: cannot initialize index\n");
        exit(EXIT_FAILURE);
    }

    //
    // output
    //
    /*
    output.nsroot = namespaces_allocate(output.zdbsettings);

    if(!(output.namespace = namespace_load_light(output.nsroot, nsname, 1))) {
        fprintf(stderr, "[-] quick-compact: output: could not initialize namespace\n");
        exit(EXIT_FAILURE);
    }

    if(!(output.zdbdata = zdb_data_init_lazy(output.zdbsettings, output.namespace->datapath, 0))) {
        fprintf(stderr, "[-] quick-compact: output: cannot load data files\n");
        exit(EXIT_FAILURE);
    }

    if(!(output.zdbindex = zdb_index_init(output.zdbsettings, output.namespace->indexpath, output.namespace, output.nsroot->branches))) {
        fprintf(stderr, "[-] quick-compact: output: cannot initialize index\n");
        exit(EXIT_FAILURE);
    }
    */

    return quick_compaction(&input, &output);
}
