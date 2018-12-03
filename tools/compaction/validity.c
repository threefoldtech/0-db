#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <inttypes.h>
#include <sys/stat.h>
#include "compaction.h"

int directory_check(char *target) {
    struct stat sb;

    if(stat(target, &sb) != 0) {
        warnp(target);
        return 1;
    }

    if(!S_ISDIR(sb.st_mode))
        return 1;

    return 0;
}

int validity_check(compaction_t *compaction) {
    char filename[256];

    // preliminary check
    // does the data directory exists
    if(directory_check(compaction->datapath)) {
        fprintf(stderr, "[-] %s: target is not a directory\n", compaction->datapath);
        return 1;
    }

    // does data namespace directory exists
    snprintf(filename, sizeof(filename), "%s/%s", compaction->datapath, compaction->namespace);
    if(directory_check(filename)) {
        fprintf(stderr, "[-] %s: target is not a directory\n", filename);
        return 1;
    }

    // does the target directory exists
    if(directory_check(compaction->targetpath)) {
        fprintf(stderr, "[-] %s: target is not a directory\n", compaction->targetpath);
        return 1;
    }

    // does the target namespace directory
    // **doesn't** exists (we want to create it, to start fresh)
    snprintf(filename, sizeof(filename), "%s/%s", compaction->targetpath, compaction->namespace);
    if(!directory_check(filename)) {
        fprintf(stderr, "[-] %s: target already exists\n", compaction->targetpath);
        dies("the namespace on the target directory should not already exists");
    }

    if(mkdir(filename, 0775))
        diep(filename);

    return 0;
}


