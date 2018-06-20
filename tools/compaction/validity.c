#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <inttypes.h>
#include <sys/stat.h>
#include "compaction.h"

int file_check(char *target) {
    struct stat sb;

    if(stat(target, &sb) != 0)
        diep(target);

    if(!S_ISREG(sb.st_mode))
        return 1;

    return 0;
}

int directory_check(char *target) {
    struct stat sb;

    if(stat(target, &sb) != 0)
        diep(target);

    if(!S_ISDIR(sb.st_mode))
        return 1;

    return 0;
}

int validity_check(compaction_t *compaction) {
    char filename[256];

    // preliminary check
    // does index path is a directory
    if(directory_check(compaction->indexpath)) {
        fprintf(stderr, "[-] %s: target is not a directory\n", compaction->indexpath);
        return 1;
    }

    // does the namespace index directory exists
    snprintf(filename, sizeof(filename), "%s/%s", compaction->indexpath, compaction->namespace);
    if(directory_check(filename)) {
        fprintf(stderr, "[-] %s: target is not a directory\n", filename);
        return 1;
    }

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

    // zdb validity check
    snprintf(filename, sizeof(filename), "%s/%s/zdb-namespace", compaction->indexpath, compaction->namespace);
    if(file_check(filename)) {
        fprintf(stderr, "[-] %s: invalid namespace descriptor\n", filename);;
        return 1;
    }

    return 0;
}


