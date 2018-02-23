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

// we keep a list of namespace currently used
// each namespace used will keep file descriptor opened
// and only once per namespace
//
// each time a client want a specific namespace,
// if this namespace is already used by someone else, we gives
// the same object back, like this they will all share the same
// state
//
// as soon as nobody use anymore one namespace, we free it
// from memory and close descriptors, waiting the the next one
static ns_root_t *nsroot = NULL;

//
// public namespace endpoint
//
int namespace_delete(unsigned char *name) {
    (void) name;

    return 0;
}

// be careful with this.
ns_root_t *namespace_get_list() {
    return nsroot;
}

namespace_t *namespace_get_default() {
    return nsroot->namespaces[0];
}

namespace_t *namespace_get(char *name) {
    for(size_t i = 0; i < nsroot->length; i++) {
        if(strcmp(nsroot->namespaces[i]->name, name) == 0)
            return nsroot->namespaces[i];
    }

    return NULL;
}

int namespace_set(unsigned char *name, int settings) {
    (void) name;
    (void) settings;

    return 0;
}

void namespace_descriptor_update(namespace_t *namespace, int fd) {
    ns_header_t header;

    debug("[+] namespaces: updating header\n");

    header.namelength = strlen(namespace->name);
    header.passlength = namespace->password ? strlen(namespace->password) : 0;
    header.maxsize = namespace->maxsize;
    header.flags = 0;

    if(namespace->public)
        header.flags |= NS_FLAGS_PUBLIC;

    if(write(fd, &header, sizeof(ns_header_t)) != sizeof(ns_header_t))
        warnp("namespace header write");

    if(write(fd, namespace->name, header.namelength) != (ssize_t) header.namelength)
        warnp("namespace header name write");

    if(namespace->password) {
        if(write(fd, namespace->password, header.passlength) != (ssize_t) header.passlength)
            warnp("namespace header pass write");
    }

    // ensure metadata are written
    fsync(fd);
}

static int namespace_descriptor_open(namespace_t *namespace) {
    char pathname[PATH_MAX];
    int fd;

    snprintf(pathname, PATH_MAX, "%s/zdb-namespace", namespace->indexpath);

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
        namespace_descriptor_update(namespace, fd);
        close(fd);
        return;
    }

    namespace->maxsize = header.maxsize;
    namespace->public = (header.flags & NS_FLAGS_PUBLIC);

    if(header.passlength) {
        if(!(namespace->password = malloc(sizeof(char) * header.passlength))) {
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

// update persistance data of a namespace
// basicly, this rewrite it's metadata on disk
void namespace_commit(namespace_t *namespace) {
    int fd;

    if((fd = namespace_descriptor_open(namespace)) < 0)
        return;

    // update metadata
    namespace_descriptor_update(namespace, fd);

    close(fd);
}

static char *namespace_path(char *prefix, char *name) {
    char pathname[PATH_MAX];
    snprintf(pathname, PATH_MAX, "%s/%s", prefix, name);

    return strdup(pathname);
}

namespace_t *namespace_ensure(namespace_t *namespace) {
    debug("[+] namespaces: checking index [%s]\n", namespace->indexpath);
    if(!dir_exists(namespace->indexpath)) {
        if(dir_create(namespace->indexpath) < 0)
            return warnp("index dir_create");
    }

    debug("[+] namespaces: checking data [%s]\n", namespace->datapath);
    if(!dir_exists(namespace->datapath)) {
        if(dir_create(namespace->datapath) < 0)
            return warnp("data dir_create");
    }

    return namespace;
}

// load (or create if it doesn't exists) a namespace
static namespace_t *namespace_load(ns_root_t *nsroot, char *name) {
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

    if(!namespace_ensure(namespace))
        return NULL;

    namespace_descriptor_load(namespace);

    // now, we are sure the namespace exists, but it's maybe empty
    // let's call index and data initializer, they will take care about that
    namespace->index = index_init(nsroot->settings, namespace->indexpath, namespace, nsroot->branches);
    namespace->data = data_init(nsroot->settings, namespace->datapath, namespace->index->indexid);

    return namespace;
}

//
// add a namespace to the main namespaces list
//
static namespace_t *namespace_push(ns_root_t *root, namespace_t *namespace) {
    size_t newlength = root->length + 1;
    namespace_t **newlist = NULL;

    if(!(newlist = realloc(root->namespaces, sizeof(namespace_t) * newlength)))
        return warnp("realloc namespaces list");

    root->namespaces = newlist;
    root->namespaces[root->length] = namespace;
    root->length = newlength;

    return namespace;
}

//
// create (load) a new namespace
//
// note: this function doesn't check if the namespace already exists
// if it already exists, you could end with duplicate in memory
// do not call this is you didn't checked the namespace already exists
// by getting it first
//
int namespace_create(char *name) {
    namespace_t *namespace;

    // call the generic namespace loader
    if(!(namespace = namespace_load(nsroot, name)))
        return 0;

    // append the namespace to the main list
    if(!namespace_push(nsroot, namespace))
        return 0;

    return 1;
}

static int namespace_valid_name(char *name) {
    if(strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
        return 0;

    if(strcmp(name, NAMESPACE_DEFAULT) == 0)
        return 0;

    return 1;
}

//
// scan the index directories and load any namespaces found
//
static int namespace_scanload(ns_root_t *root) {
    int loaded = 0;
    struct dirent *ep;
    DIR *dp;

    // listing the directory
    // if this fails, we mark this as fatal since
    // it's on init-time, if the directory cannot be read
    // we should not be here at all
    if(!(dp = opendir(root->settings->indexpath)))
        diep("opendir");

    while((ep = readdir(dp))) {
        if(!namespace_valid_name(ep->d_name))
            continue;

        debug("[+] namespaces: extra found: %s\n", ep->d_name);

        // loading the namespace
        namespace_t *namespace;
        if(!(namespace = namespace_load(root, ep->d_name)))
            continue;

        // commit to the main list
        namespace_push(root, namespace);
        loaded += 1;
    }

    closedir(dp);

    verbose("[+] namespaces: %d extra namespaces loaded\n", loaded);

    return loaded;
}

// here is where the whole initialization and loader starts
//
// for each namespace (and begin with the default one), we load
// the index and data, and prepare everything for a working system
//
// previously, there was only one index and one data set, now for each
// namespace, we load each of them separatly, but keep only one big index
// in memory (because he use a big initial array, we can't keep lot of them)
// and keep a reference to the namespace for each entries
//
// because this is the first entry point, here will be the only place where
// we knows everything about index and data, we keep every pointer and allocation
// here, in a global scope
//
// this is why it's here we take care about cleaning and emergencies, it's the only
// place where we __knows__ what we needs to clean
int namespace_init(settings_t *settings) {
    verbose("[+] namespaces: initializing\n");

    // we start by the default namespace
    if(!(nsroot = (ns_root_t *) malloc(sizeof(ns_root_t))))
        diep("namespaces malloc");

    nsroot->length = 1;             // we start with the default one, only
    nsroot->settings = settings;    // keep reference to the settings, needed for paths
    nsroot->branches = NULL;        // maybe we don't need the branches, see below

    if(!(nsroot->namespaces = (namespace_t **) malloc(sizeof(namespace_t *) * nsroot->length)))
        diep("namespace malloc");

    // allocating (if needed, only some modes needs it) the big (single) index branches
    if(settings->mode == KEYVALUE || settings->mode == SEQUENTIAL) {
        debug("[+] namespaces: pre-allocating index (%d lazy branches)\n", buckets_branches);

        // allocating minimal branches array
        if(!(nsroot->branches = (index_branch_t **) calloc(sizeof(index_branch_t *), buckets_branches)))
            diep("calloc");
    }

    // namespace 0 will always be the default one
    if(!(nsroot->namespaces[0] = namespace_load(nsroot, NAMESPACE_DEFAULT))) {
        danger("[-] could not load or create default namespace, this is fatal");
        exit(EXIT_FAILURE);
    }

    namespace_scanload(nsroot);

    return 0;
}

static void namespace_free(namespace_t *namespace) {
    free(namespace->name);
    free(namespace->indexpath);
    free(namespace->datapath);
    free(namespace->password);
    free(namespace);
}

// this is called when we receive a graceful exit request
// let's clean all index, data and namespace stuff
int namespace_destroy() {
    // freeing the big index buffer
    // since branch want an index as argument, let's use
    // the first namespace (default), since they all share
    // the same buffer
    debug("[+] namespaces: cleaning branches\n");
    for(uint32_t b = 0; b < buckets_branches; b++)
        index_branch_free(nsroot->namespaces[0]->index, b);

    // freeing the big index array
    free(nsroot->branches);

    // freeing each namespace's index and data buffers
    debug("[+] namespaces: cleaning index and data\n");
    for(size_t i = 0; i < nsroot->length; i++) {
        index_destroy(nsroot->namespaces[i]->index);
        data_destroy(nsroot->namespaces[i]->data);
        namespace_free(nsroot->namespaces[i]);
    }

    // freeing internal namespaces support
    free(nsroot->namespaces);
    nsroot->length = 0;

    free(nsroot);

    return 0;
}

int namespace_emergency() {
    for(size_t i = 0; i < nsroot->length; i++) {
        printf("[+] namespaces: flushing index [%s]\n", nsroot->namespaces[i]->name);

        if(index_emergency(nsroot->namespaces[i]->index)) {
            printf("[+] namespaces: flushing data [%s]\n", nsroot->namespaces[i]->name);
            // only flusing data if index flush was accepted
            // if index flush returns 0, we are probably in an initializing stage
            data_emergency(nsroot->namespaces[i]->data);
        }
    }

    return 0;
}
