#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include "libzdb.h"

int main(int argc, char *argv[]) {
    char *dirname = NULL;
    char *nsname = NULL;

    if(argc < 3) {
        fprintf(stderr, "Usage: %s index-rootpath namespace-name\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    // loading zdb
    printf("[*] 0-db engine v%s\n", zdb_version());

    // fetching directory path
    dirname = argv[1];
    nsname = argv[2];

    printf("[+] namespace-dump: loading [%s] from: %s\n", nsname, dirname);

    if(zdb_dir_exists(dirname) != ZDB_DIRECTORY_EXISTS) {
        fprintf(stderr, "[-] namespace-dump: could not reach index directory\n");
        exit(EXIT_FAILURE);
    }

    // initializing database
    zdb_settings_t *zdb_settings = zdb_initialize();
    zdb_settings->indexpath = dirname;

    zdb_id_set("namespace-dump");

    // lazy load namespace
    ns_root_t *nsroot = namespaces_allocate(zdb_settings);
    namespace_t *ns = namespace_load_light(nsroot, nsname, 0);

    printf("[+] ----------------------------------------\n");
    printf("[+] name        : %s\n", ns->name);
    printf("[+] password    : %s\n", ns->password ? ns->password : "<no password set>");
    printf("[+] public flag : %d\n", ns->public);
    printf("[+] mode worm   : %d\n", ns->worm);
    printf("[+] maximum size: %lu bytes (%.2f MB)\n", ns->maxsize, MB(ns->maxsize));
    printf("[+] -----------------------------------------\n");

    return 0;
}
