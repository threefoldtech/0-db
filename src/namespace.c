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
#include <errno.h>
#include <time.h>
#include "zerodb.h"
#include "index.h"
#include "index_loader.h"
#include "data.h"
#include "namespace.h"

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
static namespaces_t *nsroot = NULL;

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
    return NULL;
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

int namespace_exists(namespace_t *nsroot, char *name) {
    (void) nsroot;
    (void) name;

    return 0;
}

namespace_t *namespace_load(namespaces_t *nsroot, char *name) {
    (void) nsroot;
    (void) name;

    // printf("CREATING DEFAULT: %s\n", name);
    // exit(EXIT_FAILURE);

    return NULL;
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
    if(!(nsroot = (namespaces_t *) malloc(sizeof(namespaces_t))))
        diep("namespaces malloc");

    nsroot->length = 1;
    nsroot->settings = settings;

    if(!(nsroot->namespaces = (namespace_t **) malloc(sizeof(namespace_t *) * nsroot->length)))
        diep("namespace malloc");

    // namespace 0 will always be the default one
    // nsroot->namespaces[0] = namespace_load(nsroot->settings, NAMESPACE_DEFAULT);

    // creating the index in memory
    // this will returns us the id of the index
    // file currently used, this is needed by the data
    // storage to keep files linked (index-0067 <> data-0067)
    uint16_t indexid = index_init(settings);
    data_init(indexid, settings);

    return 0;
}

int namespace_destroy() {
    index_destroy();
    data_destroy();

    return 0;
}

int namespace_emergency() {
    printf("[+] flushing index\n");

    if(index_emergency()) {
        printf("[+] flushing data\n");
        // only flusing data if index flush was accepted
        // if index flush returns 0, we are probably in an initializing stage
        data_emergency();
    }

    return 0;
}
