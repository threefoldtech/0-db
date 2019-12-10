#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <getopt.h>
#include "libzdb.h"

static struct option long_options[] = {
    {"index",     required_argument, 0, 'f'},
    {"namespace", required_argument, 0, 'n'},
    {"password",  required_argument, 0, 'p'},
    {"maxsize",   required_argument, 0, 'm'},
    {"public",    required_argument, 0, 'P'},
    {"worm"  ,    required_argument, 0, 'w'},
    {"help",      no_argument,       0, 'h'},
    {0, 0, 0, 0}
};

int namespace_edit(char *dirname, char *nsname, namespace_t *namespace) {
    if(zdb_dir_exists(dirname) != ZDB_DIRECTORY_EXISTS) {
        fprintf(stderr, "[-] namespace-dump: could not reach index directory\n");
        exit(EXIT_FAILURE);
    }

    // initializing database
    zdb_settings_t *zdb_settings = zdb_initialize();
    zdb_settings->indexpath = dirname;

    zdb_id_set("namespace-editor");

    // lazy load namespace
    ns_root_t *nsroot = namespaces_allocate(zdb_settings);
    namespace_t *ns = namespace_load_light(nsroot, nsname, 0);

    if(!ns) {
        fprintf(stderr, "[-] could not load namespace\n");
        return 1;
    }

    printf("[+] ----------------------------------------\n");
    printf("[+]     current values    ------------------\n");
    printf("[+] ----------------------------------------\n");
    printf("[+] name        : %s\n", ns->name);
    printf("[+] password    : %s\n", ns->password ? ns->password : "<no password set>");
    printf("[+] public flag : %d\n", ns->public);
    printf("[+] worm mode   : %d\n", ns->worm);
    printf("[+] maximum size: %lu bytes (%.2f MB)\n", ns->maxsize, MB(ns->maxsize));
    printf("[+] -----------------------------------------\n");
    printf("[+]\n");
    printf("[+] writing new values\n");

    ns->name = nsname;
    ns->password = namespace->password;
    ns->public = namespace->public;
    ns->worm = namespace->worm;
    ns->maxsize = namespace->maxsize;

    // writing changes
    namespace_commit(ns);

    printf("[+]\n");
    printf("[+] ----------------------------------------\n");
    printf("[+]     updated values    ------------------\n");
    printf("[+] ----------------------------------------\n");
    printf("[+] name        : %s\n", ns->name);
    printf("[+] password    : %s\n", ns->password ? ns->password : "<no password set>");
    printf("[+] public flag : %d\n", ns->public);
    printf("[+] worm mode   : %d\n", ns->worm);
    printf("[+] maximum size: %lu bytes (%.2f MB)\n", ns->maxsize, MB(ns->maxsize));

    return 0;
}

void usage() {
    printf("Namespace editor tool arguments:\n\n");

    printf("  --index      <dir>      index root directory path (required)\n");
    printf("  --namespace  <name>     name of the namespace (required)\n");
    printf("  --password   <pass>     password (default, empty)\n");
    printf("  --maxsize    <size>     maximum size in bytes (default, no limit)\n");
    printf("  --public     <bool>     does the namespace is public or not (yes or no, default yes)\n");
    printf("  --worm       <bool>     does the namespace use WORM mode (yes or no, default no)\n");
    printf("  --help                  print this message\n");

    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int option_index = 0;
    namespace_t namespace;
    char *dirname = NULL;
    char *nsname = NULL;

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
                dirname = optarg;
                break;

            case 'n':
                nsname = optarg;
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

            case 'w':
                if(strcmp(optarg, "yes") == 0) {
                    namespace.worm = 1;

                } else if(strcmp(optarg, "no") == 0) {
                    namespace.worm = 0;

                } else {
                    fprintf(stderr, "[-] invalid value for --worm (yes or no expected)\n");
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

    if(!dirname) {
        fprintf(stderr, "[-] missing index root directory\n");
        usage();
    }

    if(!nsname) {
        fprintf(stderr, "[-] missing namespace name, you need to specify a name\n");
        usage();
    }

    printf("[+] zdb namespace editor\n");

    return namespace_edit(dirname, nsname, &namespace);
}
