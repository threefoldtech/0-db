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
int namespace_create(unsigned char *name, unsigned char *secret, int public) {
    (void) name;
    (void) secret;
    (void) public;

    return 0;
}

void namespace_list() {

}

int namespace_delete(unsigned char *name) {
    (void) name;

    return 0;
}

namespace_t *namespace_get_default() {
    return nsroot->namespaces[0];
}

namespace_t *namespace_get(unsigned char *name, unsigned char *secret) {
    (void) name;
    (void) secret;

    return NULL;
}

int namespace_set(unsigned char *name, int settings) {
    (void) name;
    (void) settings;

    return 0;
}

static char *namespace_path(char *prefix, char *name) {
    char pathname[PATH_MAX];
    snprintf(pathname, PATH_MAX, "%s/%s", prefix, name);

    return strdup(pathname);
}

namespace_t *namespace_ensure(namespace_t *namespace) {
    debug("[+] namespace: checking index [%s]\n", namespace->indexpath);
    if(!dir_exists(namespace->indexpath)) {
        if(dir_create(namespace->indexpath) < 0)
            return warnp("index dir_create");
    }

    debug("[+] namespace: checking data [%s]\n", namespace->datapath);
    if(!dir_exists(namespace->datapath)) {
        if(dir_create(namespace->datapath) < 0)
            return warnp("data dir_create");
    }

    return namespace;
}

// load (or create if it doesn't exists) a namespace
namespace_t *namespace_load(ns_root_t *nsroot, char *name) {
    namespace_t *namespace;

    debug("[+] namespace: loading '%s'\n", name);

    if(!(namespace = malloc(sizeof(namespace_t)))) {
        warnp("namespace malloc");
        return NULL;
    }

    namespace->name = strdup(name);
    namespace->password = NULL;
    namespace->indexpath = namespace_path(nsroot->settings->indexpath, name);
    namespace->datapath = namespace_path(nsroot->settings->datapath, name);

    if(!namespace_ensure(namespace))
        return NULL;

    // now, we are sure the namespace exists, but it's maybe empty
    // let's call index and data initializer, they will take care about that
    namespace->index = index_init(nsroot->settings, namespace->indexpath, nsroot->branches);
    namespace->data = data_init(nsroot->settings, namespace->datapath, namespace->index->indexid);

    return namespace;
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
        debug("[+] pre-allocating index (%d lazy branches)\n", buckets_branches);

        // allocating minimal branches array
        if(!(nsroot->branches = (index_branch_t **) calloc(sizeof(index_branch_t *), buckets_branches)))
            diep("calloc");
    }

    // namespace 0 will always be the default one
    if(!(nsroot->namespaces[0] = namespace_load(nsroot, NAMESPACE_DEFAULT))) {
        danger("[-] could not load or create default namespace, this is fatal");
        exit(EXIT_FAILURE);
    }

    // TODO: scanning for others namespaces now

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
    debug("[+] cleaning branches\n");
    for(uint32_t b = 0; b < buckets_branches; b++)
        index_branch_free(nsroot->namespaces[0]->index, b);

    // freeing the big index array
    free(nsroot->branches);

    // freeing each namespace's index and data buffers
    debug("[+] cleaning index and data\n");
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
    printf("[+] flushing index\n");

    if(index_emergency(nsroot->namespaces[0]->index)) {
        printf("[+] flushing data\n");
        // only flusing data if index flush was accepted
        // if index flush returns 0, we are probably in an initializing stage
        data_emergency(nsroot->namespaces[0]->data);
    }

    return 0;
}
