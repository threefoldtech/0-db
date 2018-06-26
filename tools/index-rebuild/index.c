#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <limits.h>
#include <x86intrin.h>
#include "zerodb.h"
#include "index.h"
#include "index_loader.h"
#include "index_branch.h"
#include "data.h"
#include "hook.h"

// wrap (mostly) all write operation on indexfile
// it's easier to keep a single logic with error handling
// related to write check
int index_write(int fd, void *buffer, size_t length, index_root_t *root) {
    (void) root;
    ssize_t response;

    if((response = write(fd, buffer, length)) < 0) {
        warnp("index write");
        return 0;
    }

    if(response != (ssize_t) length) {
        fprintf(stderr, "[-] index write: partial write\n");
        return 0;
    }

    return 1;
}

// set global filename based on the index id
void index_set_id(index_root_t *root) {
    sprintf(root->indexfile, "%s/zdb-index-%05u", root->indexdir, root->indexid);
}

// open the current filename set on the global struct
void index_open_final(index_root_t *root) {
    int flags = O_CREAT | O_RDWR | O_APPEND;

    if(root->status & INDEX_READ_ONLY)
        flags = O_RDONLY;

    if((root->indexfd = open(root->indexfile, flags, 0600)) < 0) {
        warnp(root->indexfile);
        fprintf(stderr, "[-] index: could not open index file\n");
        return;
    }

    printf("[+] index: active file: %s\n", root->indexfile);
}
