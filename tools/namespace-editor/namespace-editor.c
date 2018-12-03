#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <getopt.h>
#include "zerodb.h"
#include "index.h"
#include "data.h"
#include "namespace.h"

static struct option long_options[] = {
    {"index",     required_argument, 0, 'f'},
    {"namespace", required_argument, 0, 'n'},
    {"password",  required_argument, 0, 'p'},
    {"maxsize",   required_argument, 0, 'm'},
    {"public",    required_argument, 0, 'P'},
    {"help",      no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

void *warnp(char *str) {
    fprintf(stderr, "[-] %s: %s\n", str, strerror(errno));
    return NULL;
}

void diep(char *str) {
    warnp(str);
    exit(EXIT_FAILURE);
}

void dies(char *str) {
    fprintf(stderr, "[-] %s\n", str);
    exit(EXIT_FAILURE);
}

int directory_check(char *target) {
    struct stat sb;

    if(stat(target, &sb) != 0)
        return 1;

    if(!S_ISDIR(sb.st_mode))
        return 2;

    return 0;
}

int namespace_edit(namespace_t *namespace) {
    char *fullpath;
    ns_header_t header;
    int fd;

    if(asprintf(&fullpath, "%s/%s/zdb-namespace", namespace->indexpath, namespace->name) < 0)
        diep("asprintf");

    if((fd = open(fullpath, O_WRONLY | O_TRUNC | O_CREAT, 0600)) < 0)
        diep(fullpath);

    // fill-in the struct
    header.namelength = strlen(namespace->name);
    header.passlength = namespace->password ? strlen(namespace->password) : 0;
    header.maxsize = namespace->maxsize;
    header.flags = namespace->public ? NS_FLAGS_PUBLIC : 0;

    if(write(fd, &header, sizeof(ns_header_t)) != sizeof(ns_header_t))
        diep("write struct");

    ssize_t length = (ssize_t) strlen(namespace->name);

    if(write(fd, namespace->name, length) != length)
        diep("write name");

    if(namespace->password) {
        length = strlen(namespace->password);

        if(write(fd, namespace->password, length) != length)
            diep("write password");
    }

    length = lseek(fd, SEEK_CUR, 0);

    printf("[+] namespace descriptor written (%ld bytes)\n", length);

    close(fd);

    return 0;
}

void usage() {
    printf("Index rebuild tool arguments:\n\n");

    printf("  --index      <dir>      index root directory path (required)\n");
    printf("  --namespace  <name>     name of the namespace (required)\n");
    printf("  --password   <pass>     password (default, empty)\n");
    printf("  --maxsize    <size>     maximum size in bytes (default, no limit)\n");
    printf("  --public     <bool>     does the namespace is public or not (yes or no, default yes)\n");
    printf("  --help                  print this message\n");

    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int option_index = 0;
    namespace_t namespace;
    char *nspath;

    // initializing everything to zero
    memset(&namespace, 0x00, sizeof(namespace_t));

    // set namespace to be public by default
    namespace.public = 1;

    while(1) {
        // int i = getopt_long_only(argc, argv, "d:i:l:p:vxh", long_options, &option_index);
        int i = getopt_long_only(argc, argv, "", long_options, &option_index);

        if(i == -1)
            break;

        switch(i) {
            case 'f':
                namespace.indexpath = optarg;
                break;

            case 'n':
                namespace.name = optarg;
                break;

            case 'p':
                namespace.password = optarg;
                break;

            case 'm':
                namespace.maxsize = atoll(optarg);
                break;

            case 'P':
                if(strcmp(optarg, "yes") == 0) {
                    namespace.public = 1;

                } else if(strcmp(optarg, "no") == 0) {
                    namespace.public = 0;

                } else {
                    fprintf(stderr, "[-] invalid value for --public (yes or no expected)\n");
                    usage();
                }

                break;

            case 'h':
                usage();
                break;

            case '?':
            default:
               exit(EXIT_FAILURE);
        }
    }

    if(!namespace.indexpath) {
        fprintf(stderr, "[-] missing index root directory\n");
        usage();
    }

    if(!namespace.name) {
        fprintf(stderr, "[-] missing namespace name, you need to specify a name\n");
        usage();
    }

    if(directory_check(namespace.indexpath)) {
        if(mkdir(namespace.indexpath, 0775) < 0)
            diep(namespace.indexpath);
    }

    if(asprintf(&nspath, "%s/%s", namespace.indexpath, namespace.name) < 0)
        diep("asprintf");

    if(directory_check(nspath)) {
        if(mkdir(nspath, 0775) < 0)
            diep(nspath);
    }

    printf("[+] zdb namespace editor\n");
    printf("[+] directory: %s\n", namespace.indexpath);
    printf("[+] namespace: %s\n", namespace.name);
    printf("[+] public   : %s\n", namespace.public ? "yes" : "no");
    printf("[+] password : %s\n", namespace.password ? "<protected>" : "<not set>");
    printf("[+] max size : %.2f MB\n", MB(namespace.maxsize));

    return namespace_edit(&namespace);
}
