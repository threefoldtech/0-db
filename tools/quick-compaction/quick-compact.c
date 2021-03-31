#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>
#include <getopt.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
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

void diep(char *str) {
    perror(str);
    exit(EXIT_FAILURE);
}

typedef struct instance_t {
    zdb_settings_t *zdbsettings;    // only available for input
    char *nsname;                   // used for output since zdb not avaible

    char *indexpath;
    index_root_t *zdbindex;

    char *datapath;
    data_root_t *zdbdata;

    ns_root_t *nsroot;
    namespace_t *namespace;

} instance_t;

typedef struct session_t {
    // keep track of the previous offset
    // across files
    uint32_t dataprev;

} session_t;

int index_data_jump_to(fileid_t fileid, index_root_t *zdbindex, data_root_t *zdbdata) {
    data_header_t *header;
    char datestr[64];

    // closing previoud index fd
    if(zdbindex->indexfd)
        close(zdbindex->indexfd);

    // closing previous data fd
    if(zdbdata->datafd)
        close(zdbdata->datafd);

    printf("[+] quick-compact: opening indexfile id: %d\n", fileid);

    // opening index file
    zdbindex->indexid = fileid;
    if(index_open_readonly(zdbindex, fileid) < 0)
        return 1;

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

static int quick_dir_ensure(char *root, char *subdir) {
    char buffer[512];

    snprintf(buffer, sizeof(buffer), "%s/%s", root, subdir);

    if(zdb_dir_exists(buffer) != ZDB_DIRECTORY_EXISTS) {
        if(zdb_dir_create(buffer) < 0) {
            diep(buffer);
        }
    }

    return 0;
}

static int quick_descriptor_copy(instance_t *input, instance_t *output) {
    char pathname[ZDB_PATH_MAX];
    int fd;

    snprintf(pathname, ZDB_PATH_MAX, "%s/%s/zdb-namespace", output->indexpath, output->nsname);

    if((fd = open(pathname, O_CREAT | O_RDWR, 0600)) < 0)
        diep(pathname);

    namespace_descriptor_update(input->namespace, fd);
    close(fd);

    return 0;
}

static int quick_initialize(instance_t *input, instance_t *output) {
    printf("[+] quick-compact: initialize output environment\n");

    // create output directories
    quick_dir_ensure(output->indexpath, output->nsname);
    quick_dir_ensure(output->datapath, output->nsname);

    // copy namespace descriptor
    quick_descriptor_copy(input, output);

    return 0;
}

size_t quick_compact_pass(instance_t *input, instance_t *output, fileid_t fileid, session_t *session) {
    size_t entrycount = 0;
    int indexfd, datafd;
    char buffer[2048];
    char filebuf[512];
    char id[512];

    // temporarily fd
    int transfd = 0;
    int bkfd = 0;

    // original data offset
    uint32_t dataoffset = sizeof(data_header_t);

    // create target index and data files
    snprintf(filebuf, sizeof(filebuf), "%s/%s/zdb-index-%05d", output->indexpath, output->nsname, fileid);

    printf("[+] quick-compact: creating target index: %s\n", filebuf);
    if((indexfd = open(filebuf, O_CREAT | O_RDWR, 0600)) < 0)
        diep(filebuf);

    snprintf(filebuf, sizeof(filebuf), "%s/%s/zdb-data-%05d", output->datapath, output->nsname, fileid);

    printf("[+] quick-compact: creating target data: %s\n", filebuf);
    if((datafd = open(filebuf, O_CREAT | O_RDWR, 0600)) < 0)
        diep(filebuf);

    // ensure all fd are in the begining of files
    lseek(input->zdbindex->indexfd, 0, SEEK_SET);
    lseek(input->zdbdata->datafd, 0, SEEK_SET);

    // copy index header
    printf("[+] quick-compact: copying index header\n");
    if(read(input->zdbindex->indexfd, buffer, sizeof(index_header_t)) != sizeof(index_header_t))
        diep("index header read");

    if(write(indexfd, buffer, sizeof(index_header_t)) != sizeof(index_header_t))
        diep("index header write");

    // copy data header
    printf("[+] quick-compact: copying data header\n");
    if(read(input->zdbdata->datafd, buffer, sizeof(data_header_t)) != sizeof(data_header_t))
        diep("data header read");

    if(write(datafd, buffer, sizeof(data_header_t)) != sizeof(data_header_t))
        diep("data header write");


    // processing each entries in index
    // FIXME: read the whole index in memory and parse it in memory
    //        this will be way much faster
    while(read(input->zdbindex->indexfd, buffer, sizeof(index_item_t)) == sizeof(index_item_t)) {
        index_item_t *item = (index_item_t *) buffer;
        uint32_t truelen = item->length;
        uint32_t trueoff = item->offset;
        uint32_t truedid = item->dataid;

        // fetch id
        if(read(input->zdbindex->indexfd, id, item->idlength) != item->idlength)
            diep("index read id");

        printf("[+] quick-compact: processing new entry\n");
        index_item_header_dump(item);

        if(item->flags & INDEX_ENTRY_DELETED) {
            printf(">> DISCARD THIS ENTRY\n");
            item->length = 0;
        }

        // update offset which could be modified by
        // previous truncation
        item->offset = dataoffset;

        // always reset dataid to this fileid
        // it's possible that in sequential mode, data are not
        // on the same id that this index id (old key overwritten)
        // but we rewrite it to this id for sure
        item->dataid = fileid;

        // copy item to destination
        if(write(indexfd, buffer, sizeof(index_item_t)) != sizeof(index_item_t))
            diep("write index entry");

        if(write(indexfd, id, item->idlength) != item->idlength)
            diep("write index id");

        //
        // transfert data (or truncate it)
        //

        data_entry_header_t *dataentry;
        ssize_t datalen = sizeof(data_entry_header_t) + item->idlength + truelen;

        if(!(dataentry = malloc(datalen)))
            diep("data entry malloc");

        // opening new datafile if expected one is not the right one
        if(truedid != fileid) {
            printf("[+] quick-compact: opening another datafile\n");

            snprintf(filebuf, sizeof(filebuf), "%s/%s/zdb-data-%05d", input->datapath, output->nsname, truedid);

            if((transfd = open(filebuf, O_RDONLY, 0600)) < 0)
                diep(buffer);

            bkfd = input->zdbdata->datafd;
            input->zdbdata->datafd = transfd;
        }

        // moving datafile to the expected location
        // when we delete entries, we add a new entry on the datafile
        // so walking over datafile doesn't mean we are reading the
        // expected key, we need to jump to the expected offset from index
        lseek(input->zdbdata->datafd, trueoff, SEEK_SET);

        if(read(input->zdbdata->datafd, dataentry, datalen) != datalen)
            diep("data payload read");

        // closing temporarily opened dataid if any
        if(transfd == input->zdbdata->datafd) {
            printf("[+] quick-compact: rollback to original datafile\n");
            input->zdbdata->datafd = bkfd;
            close(transfd);
            transfd = 0;
        }

        printf("original previous = %d\n", dataentry->previous);
        printf("original id len = %d\n", dataentry->idlength);
        printf("original data len = %d\n", dataentry->datalength);

        if(item->length == 0) {
            dataentry->datalength = 0;
            dataentry->flags |= DATA_ENTRY_TRUNCATED;
            dataentry->integrity = 0;
        }

        // always update previous
        dataentry->previous = session->dataprev;
        printf("new previous = %d\n", dataentry->previous);

        ssize_t writelen = sizeof(data_entry_header_t) + item->idlength + item->length;

        // saving current position as previous entry
        session->dataprev = lseek(datafd, 0, SEEK_CUR);
        dataoffset = session->dataprev;

        // writing new data
        if(write(datafd, dataentry, writelen) != writelen)
            diep("data payload write");

        printf("<<<< new next offset: %lu\n", lseek(datafd, 0, SEEK_CUR));
        free(dataentry);

        entrycount += 1;
    }

    close(indexfd);
    close(datafd);

    printf("[+] quick-compact: index pass entries: %lu\n", entrycount);

    return entrycount;
}

int quick_compaction(instance_t *input, instance_t *output) {
    fileid_t fileid = 0;
    size_t entrycount = 0;
    session_t session = {
        .dataprev = 0,
    };

    quick_initialize(input, output);

    // prorcessing all files
    for(fileid = 0; ; fileid += 1) {
        // setting index and data id to new id
        if(index_data_jump_to(fileid, input->zdbindex, input->zdbdata))
            break;

        // processing this file
        entrycount += quick_compact_pass(input, output, fileid, &session);
    }

    printf("[+] compaction done (%lu entries inserted)\n", entrycount);

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
    output.nsname = nsname;

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
