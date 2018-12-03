#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "zerodb.h"
#include "index.h"

//
// initialize an index file
// this basicly create the header and write it
//
index_header_t index_initialize(int fd, uint16_t indexid, index_root_t *root) {
    index_header_t header;

    memcpy(header.magic, "IDX0", 4);
    header.version = ZDB_IDXFILE_VERSION;
    header.created = time(NULL);
    header.fileid = indexid;
    header.opened = time(NULL);
    header.mode = 0; // FIXME

    if(!index_write(fd, &header, sizeof(index_header_t), root))
        diep("index_initialize: write");

    return header;
}
