#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <time.h>
#include "zerodb.h"
#include "index.h"
#include "index_branch.h"
#include "index_loader.h"
#include "data.h"
#include "namespace.h"
#include "filesystem.h"
#include "redis.h"
#include "hook.h"

static int namespace_descriptor_open(namespace_t *namespace) {
    char pathname[ZDB_PATH_MAX];
    int fd;

    snprintf(pathname, ZDB_PATH_MAX, "%s/zdb-namespace", namespace->indexpath);

    if((fd = open(pathname, O_CREAT | O_RDWR, 0600)) < 0) {
        warning("[-] cannot create or open in read-write the namespace file\n");
        return -1;
    }

    return fd;
}

// read (or create) a namespace descriptor
// namespace descriptor is a binary file containing namespace
// specification such password, maxsize, etc. (see header)
static void namespace_descriptor_load(namespace_t *namespace) {
    ns_header_t header;
    int fd;

    if((fd = namespace_descriptor_open(namespace)) < 0)
        return;

    if(read(fd, &header, sizeof(ns_header_t)) != sizeof(ns_header_t)) {
        // probably new file, let's write initial namespace information
        close(fd);
        return;
    }

    namespace->maxsize = header.maxsize;
    namespace->public = (header.flags & NS_FLAGS_PUBLIC);

    if(header.passlength) {
        if(!(namespace->password = calloc(sizeof(char), header.passlength + 1))) {
            warnp("namespace password malloc");
            return;
        }

        // skipping the namespace name, jumping to password
        lseek(fd, header.namelength, SEEK_CUR);

        if(read(fd, namespace->password, header.passlength) != (ssize_t) header.passlength)
            warnp("namespace password read");
    }

    debug("[+] namespace '%s': maxsize: %lu\n", namespace->name, namespace->maxsize);
    debug("[+] -> password protection: %s\n", namespace->password ? "yes" : "no");
    debug("[+] -> public access: %s\n", namespace->public ? "yes" : "no");

    close(fd);
}

static char *namespace_path(char *prefix, char *name) {
    char pathname[ZDB_PATH_MAX];
    snprintf(pathname, ZDB_PATH_MAX, "%s/%s", prefix, name);

    return strdup(pathname);
}


// load (or create if it doesn't exists) a namespace
namespace_t *namespace_load_light(ns_root_t *nsroot, char *name) {
    namespace_t *namespace;

    debug("[+] namespaces: loading '%s'\n", name);

    if(!(namespace = malloc(sizeof(namespace_t)))) {
        warnp("namespace malloc");
        return NULL;
    }

    namespace->name = strdup(name);
    namespace->password = NULL;  // no password by default, need to be set later
    namespace->indexpath = namespace_path(nsroot->settings->indexpath, name);
    namespace->datapath = namespace_path(nsroot->settings->datapath, name);
    namespace->public = 1;  // by default, namespace are public (no password)
    namespace->maxsize = 0; // by default, there is no limits
    namespace->idlist = 0;  // by default, no list set

    // load descriptor from disk
    namespace_descriptor_load(namespace);

    return namespace;
}

ns_root_t *namespaces_allocate(settings_t *settings) {
    ns_root_t *root;

    // we start by the default namespace
    if(!(root = (ns_root_t *) malloc(sizeof(ns_root_t))))
        diep("namespaces malloc");

    root->length = 1;             // we start with the default one, only
    root->effective = 1;          // no namespace really loaded yet
    root->settings = settings;    // keep reference to the settings, needed for paths
    root->branches = NULL;        // maybe we don't need the branches, see below

    if(!(root->namespaces = (namespace_t **) malloc(sizeof(namespace_t *) * root->length)))
        diep("namespace malloc");

    // allocating (if needed, only some modes needs it) the big (single) index branches
    if(settings->mode == KEYVALUE || settings->mode == SEQUENTIAL) {
        debug("[+] namespaces: pre-allocating index (%d lazy branches)\n", buckets_branches);

        // allocating minimal branches array
        if(!(root->branches = (index_branch_t **) calloc(sizeof(index_branch_t *), buckets_branches)))
            diep("calloc");
    }

    return root;
}

