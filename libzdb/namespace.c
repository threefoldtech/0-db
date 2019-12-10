#define _GNU_SOURCE
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
#include "libzdb.h"
#include "libzdb_private.h"

// we keep a list of namespace currently used
// each namespace used will keep file descriptor opened
// and only once per namespace
//
// each time a client want a specific namespace,
// if this namespace is already used by someone else, we gives
// the same object back, like this they will all share the same
// state
static ns_root_t *nsroot = NULL;

//
// public namespace endpoint
//

// iterate over available namespaces
size_t namespace_length() {
    return nsroot->effective;
}

namespace_t *namespace_iter() {
    return nsroot->namespaces[0];
}

namespace_t *namespace_iter_next(namespace_t *namespace) {
    for(size_t i = namespace->idlist + 1; i < nsroot->length; i++) {
        // skipping empty slot
        if(nsroot->namespaces[i])
            return nsroot->namespaces[i];
    }

    return NULL;
}

// getters
//
// get the default namespace
// there is always at least one namespace (the default one)
namespace_t *namespace_get_default() {
    return nsroot->namespaces[0];
}

// get a namespace from it's name
namespace_t *namespace_get(char *name) {
    namespace_t *ns;

    for(ns = namespace_iter(); ns; ns = namespace_iter_next(ns)) {
        if(strcmp(ns->name, name) == 0)
            return ns;
    }

    return NULL;
}

void namespace_descriptor_update(namespace_t *namespace, int fd) {
    ns_header_legacy_t header;
    ns_header_extended_t extended;

    zdb_debug("[+] namespaces: updating header\n");

    // legacy
    header.namelength = strlen(namespace->name);
    header.passlength = namespace->password ? strlen(namespace->password) : 0;
    header.maxsize = 0; // not used anymore
    header.flags = NS_FLAGS_EXTENDED;

    if(namespace->public)
        header.flags |= NS_FLAGS_PUBLIC;

    if(namespace->worm)
        header.flags |= NS_FLAGS_WORM;

    if(write(fd, &header, sizeof(ns_header_legacy_t)) != sizeof(ns_header_legacy_t))
        zdb_warnp("namespace legacy header write");

    if(write(fd, namespace->name, header.namelength) != (ssize_t) header.namelength)
        zdb_warnp("namespace header name write");

    if(namespace->password) {
        if(write(fd, namespace->password, header.passlength) != (ssize_t) header.passlength)
            zdb_warnp("namespace header password write");
    }

    // extended
    extended.version = namespace->version;
    extended.maxsize = namespace->maxsize;

    if(write(fd, &extended, sizeof(ns_header_extended_t)) != sizeof(ns_header_extended_t))
        zdb_warnp("namespace extended header write");

    // ensure metadata are written
    fsync(fd);
}

// upgrade a descriptor to support extended fields
void namespace_descriptor_upgrade(ns_header_legacy_t *header, int fd) {
    ns_header_extended_t extended;

    zdb_debug("[+] namespaces: upgrading header (extending)\n");

    // enable extended flag
    header->flags |= NS_FLAGS_EXTENDED;

    // initialize extended settings
    extended.version = NAMESPACE_CURRENT_VERSION;
    extended.maxsize = 0;

    // port old maxsize to new extended header
    // only if the value is less than 2 GB, which was
    // the previous hard limit
    if(header->maxsize < (((uint32_t) 1 << 31)))
        extended.maxsize = header->maxsize;

    // rollback to the beginin of the file
    lseek(fd, 0, SEEK_SET);

    // rewriting legacy struct with extended flag enabled
    if(write(fd, header, sizeof(ns_header_legacy_t)) != sizeof(ns_header_legacy_t))
        zdb_warnp("namespace legacy header write");

    // jump to extended position
    ssize_t skip = sizeof(ns_header_legacy_t) + header->namelength + header->passlength;
    lseek(fd, skip, SEEK_SET);

    // writing extended struct
    if(write(fd, &extended, sizeof(ns_header_extended_t)) != sizeof(ns_header_extended_t))
        zdb_warnp("namespace extended header write");

    // ensure metadata are written
    fsync(fd);
}

static int namespace_descriptor_open(namespace_t *namespace) {
    char pathname[ZDB_PATH_MAX];
    int fd;

    snprintf(pathname, ZDB_PATH_MAX, "%s/zdb-namespace", namespace->indexpath);

    if((fd = open(pathname, O_CREAT | O_RDWR, 0600)) < 0) {
        zdb_warning("[-] cannot create or open in read-write the namespace file");
        return -1;
    }

    return fd;
}

// read (or create) a namespace descriptor
// namespace descriptor is a binary file containing namespace
// specification such password, maxsize, etc. (see header)
static int namespace_descriptor_load(namespace_t *namespace) {
    ns_header_legacy_t header;
    ns_header_extended_t extended;
    int fd;

    if((fd = namespace_descriptor_open(namespace)) < 0)
        return fd;

    if(read(fd, &header, sizeof(ns_header_legacy_t)) != sizeof(ns_header_legacy_t)) {
        // probably new file, let's write initial namespace information
        namespace_descriptor_update(namespace, fd);
        close(fd);
        return 0;
    }

    // retro-compatibility with old format
    if((header.flags & NS_FLAGS_EXTENDED) == 0) {
        zdb_warning("[-] WARNING: updating namespace header");
        zdb_warning("[-] WARNING: descriptor was created using old 0-db version");
        zdb_warning("[-] WARNING: updates are retro-compatible");

        namespace_descriptor_upgrade(&header, fd);
    }

    // extended is set, reading extended struct
    ssize_t skip = sizeof(ns_header_legacy_t) + header.namelength + header.passlength;
    lseek(fd, skip, SEEK_SET);

    if(read(fd, &extended, sizeof(ns_header_extended_t)) != sizeof(ns_header_extended_t)) {
        zdb_warnp("namespace extended read");
        return 0;
    }

    namespace->maxsize = extended.maxsize;
    namespace->public = (header.flags & NS_FLAGS_PUBLIC);
    namespace->worm = (header.flags & NS_FLAGS_WORM);
    namespace->version = extended.version;

    if(header.passlength) {
        if(!(namespace->password = calloc(sizeof(char), header.passlength + 1))) {
            zdb_warnp("namespace password malloc");
            return 0;
        }

        // skipping the namespace name, jumping to password
        lseek(fd, skip - header.passlength, SEEK_SET);

        if(read(fd, namespace->password, header.passlength) != (ssize_t) header.passlength)
            zdb_warnp("namespace password read");
    }

    zdb_success("[+] namespace: loaded: %s", namespace->name);
    zdb_debug("[+] -> maxsize: %lu (%.2f MB)\n", namespace->maxsize, MB(namespace->maxsize));
    zdb_debug("[+] -> password protection: %s\n", namespace->password ? "yes" : "no");
    zdb_debug("[+] -> public access: %s\n", namespace->public ? "yes" : "no");
    zdb_debug("[+] -> worm mode: %s\n", namespace->worm ? "yes" : "no");

    close(fd);

    return 1;
}

// update persistance data of a namespace
// basicly, this rewrite it's metadata on disk
int namespace_commit(namespace_t *namespace) {
    int fd;

    if((fd = namespace_descriptor_open(namespace)) < 0)
        return fd;

    // update metadata
    namespace_descriptor_update(namespace, fd);

    close(fd);

    return 0;
}

static char *namespace_path(char *prefix, char *name) {
    char *pathname;

    if(asprintf(&pathname, "%s/%s", prefix, name) < 0) {
        zdb_warnp("asprintf");
        return NULL;
    }

    return pathname;
}

namespace_t *namespace_ensure(namespace_t *namespace) {
    zdb_debug("[+] namespaces: checking index [%s]\n", namespace->indexpath);
    if(zdb_dir_exists(namespace->indexpath) != ZDB_DIRECTORY_EXISTS) {
        if(zdb_dir_create(namespace->indexpath) < 0)
            return zdb_warnp("index dir_create");
    }

    zdb_debug("[+] namespaces: checking data [%s]\n", namespace->datapath);
    if(zdb_dir_exists(namespace->datapath) != ZDB_DIRECTORY_EXISTS) {
        if(zdb_dir_create(namespace->datapath) < 0)
            return zdb_warnp("data dir_create");
    }

    return namespace;
}

// lazy load a namespace
// this just populate data and index from disk
// based on an existing namespace object
// this can be used to load and reload a namespace
static int namespace_load_lazy(ns_root_t *nsroot, namespace_t *namespace) {
    // now, we are sure the namespace exists, but it's maybe empty
    // let's call index and data initializer, they will take care about that
    namespace->index = index_init(nsroot->settings, namespace->indexpath, namespace, nsroot->branches);
    namespace->data = data_init(nsroot->settings, namespace->datapath, namespace->index->indexid);

    return 0;
}

// load (or create if it doesn't exists) a namespace

namespace_t *namespace_load_light(ns_root_t *nsroot, char *name, int ensure) {
    namespace_t *namespace;

    zdb_debug("[+] namespaces: loading '%s'\n", name);

    if(!(namespace = malloc(sizeof(namespace_t)))) {
        zdb_warnp("namespace malloc");
        return NULL;
    }

    namespace->name = strdup(name);
    namespace->password = NULL;  // no password by default, need to be set later
    namespace->indexpath = namespace_path(nsroot->settings->indexpath, name);
    namespace->datapath = namespace_path(nsroot->settings->datapath, name);
    namespace->public = 1;  // by default, namespace are public (no password)
    namespace->worm = 0;    // by default, worm mode is disabled
    namespace->maxsize = 0; // by default, there is no limits
    namespace->idlist = 0;  // by default, no list set
    namespace->version = NAMESPACE_CURRENT_VERSION;

    if(ensure) {
        if(!namespace_ensure(namespace))
            return NULL;
    }

    // load descriptor from disk
    if(namespace_descriptor_load(namespace) < 0) {
        namespace_free(namespace);
        return NULL;
    }

    return namespace;
}

namespace_t *namespace_load(ns_root_t *nsroot, char *name) {
    namespace_t *namespace;

    // basic namespace loader/creation
    if(!(namespace = namespace_load_light(nsroot, name, 1)))
        return NULL;

    // memory populating
    namespace_load_lazy(nsroot, namespace);

    return namespace;
}


//
// add a namespace to the main namespaces list
//
static namespace_t *namespace_push(ns_root_t *root, namespace_t *namespace) {
    size_t newlength = root->length + 1;
    namespace_t **newlist = NULL;

    // looking for an empty namespace slot
    for(size_t i = 0; i < root->length; i++) {
        if(root->namespaces[i])
            continue;

        zdb_debug("[+] namespace: empty slot reusable found: %lu\n", i);

        // empty slot found, updating
        namespace->idlist = i;
        root->namespaces[i] = namespace;
        root->effective += 1;

        return namespace;
    }

    zdb_debug("[+] namespace: allocating new slot\n");

    // no empty slot, allocating a new one
    if(!(newlist = realloc(root->namespaces, sizeof(namespace_t) * newlength)))
        return zdb_warnp("realloc namespaces list");

    // set list id
    namespace->idlist = root->length;

    // insert to list
    root->namespaces = newlist;
    root->namespaces[root->length] = namespace;
    root->length = newlength;

    // one new effective namespace
    root->effective += 1;

    return namespace;
}

static void namespace_create_hook(namespace_t *namespace) {
    if(!zdb_rootsettings.hook)
        return;

    hook_t *hook = hook_new("namespace-created", 2);
    hook_append(hook, zdb_rootsettings.zdbid ? zdb_rootsettings.zdbid : "unknown-id");
    hook_append(hook, namespace->name);
    hook_execute(hook);
    hook_free(hook);
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

    // hook notification
    namespace_create_hook(namespace);

    return 1;
}

int namespace_valid_name(char *name) {
    if(strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
        return 0;

    if(strchr(name, '/'))
        return 0;

    // FIXME: better support (slash, coma, ...)

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
        zdb_diep("opendir");

    while((ep = readdir(dp))) {
        if(!namespace_valid_name(ep->d_name))
            continue;

        zdb_debug("[+] namespaces: extra found: %s\n", ep->d_name);

        // loading the namespace
        namespace_t *namespace;
        if(!(namespace = namespace_load(root, ep->d_name)))
            continue;

        // commit to the main list
        namespace_push(root, namespace);
        loaded += 1;
    }

    closedir(dp);

    zdb_verbose("[+] namespaces: %d extra namespaces loaded\n", loaded);

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
ns_root_t *namespaces_allocate(zdb_settings_t *settings) {
    ns_root_t *root;

    // we start by the default namespace
    if(!(root = (ns_root_t *) malloc(sizeof(ns_root_t))))
        zdb_diep("namespaces malloc");

    root->length = 1;             // we start with the default one, only
    root->effective = 1;          // no namespace really loaded yet
    root->settings = settings;    // keep reference to the settings, needed for paths
    root->branches = NULL;        // maybe we don't need the branches, see below

    if(!(root->namespaces = (namespace_t **) malloc(sizeof(namespace_t *) * root->length)))
        zdb_diep("namespace malloc");

    // allocating (if needed, only some modes needs it) the big (single) index branches
    if(settings->mode == ZDB_MODE_KEY_VALUE) {
        zdb_debug("[+] namespaces: pre-allocating index (%d lazy branches)\n", buckets_branches);

        // allocating minimal branches array
        if(!(root->branches = index_buckets_init()))
            zdb_diep("buckets allocation");
    }

    return root;
}

int namespaces_init(zdb_settings_t *settings) {
    zdb_verbose("[+] namespaces: initializing\n");

    // allocating global namespaces
    nsroot = namespaces_allocate(settings);

    // namespace 0 will always be the default one
    if(!(nsroot->namespaces[0] = namespace_load(nsroot, NAMESPACE_DEFAULT))) {
        zdb_danger("[-] could not load or create default namespace, this is fatal");
        exit(EXIT_FAILURE);
    }

    namespace_scanload(nsroot);

    return 0;
}

void namespace_free(namespace_t *namespace) {
    free(namespace->name);
    free(namespace->indexpath);
    free(namespace->datapath);
    free(namespace->password);
    free(namespace);
}

// this is called when we receive a graceful exit request
// let's clean all index, data and namespace stuff
int namespaces_destroy() {
    // freeing the big index buffer
    // since branch want an index as argument, let's use
    // the first namespace (default), since they all share
    // the same buffer
    if(nsroot->branches) {
        zdb_debug("[+] namespaces: cleaning branches\n");
        for(uint32_t b = 0; b < buckets_branches; b++)
            index_branch_free(nsroot->namespaces[0]->index->branches, b);

        // freeing the big index array
        free(nsroot->branches);
    }

    // freeing each namespace's index and data buffers
    zdb_debug("[+] namespaces: cleaning index and data\n");

    namespace_t *ns;
    for(ns = namespace_iter(); ns; ns = namespace_iter_next(ns)) {
        index_destroy(ns->index);
        data_destroy(ns->data);
    }

    // freeing all slots
    for(size_t i = 0; i < nsroot->length; i++) {
        if(nsroot->namespaces[i])
            namespace_free(nsroot->namespaces[i]);
    }

    // clean globally allocated index stuff
    index_destroy_global();

    // freeing internal namespaces support
    free(nsroot->namespaces);
    nsroot->length = 0;

    free(nsroot);

    return 0;
}

static void namespace_kick_slot(namespace_t *namespace) {
    for(size_t i = 0; i < nsroot->length; i++) {
        if(nsroot->namespaces[i] == namespace) {
            // freeing this namespace slot
            nsroot->namespaces[i] = NULL;
            return;
        }
    }
}

// trigger hook when namespace is reloaded
static void namespace_reload_hook(namespace_t *namespace) {
    if(!zdb_rootsettings.hook)
        return;

    hook_t *hook = hook_new("namespace-reloaded", 2);
    hook_append(hook, zdb_rootsettings.zdbid ? zdb_rootsettings.zdbid : "unknown-id");
    hook_append(hook, namespace->name);
    hook_execute(hook);
    hook_free(hook);
}

// start a namespace reload procees
// when reloading a namespace, we destroy it from
// memory then reload contents, we don't change anything related
// to the namespace object itself (this object is linked to users)
// we only refresh data and index pointers
int namespace_reload(namespace_t *namespace) {
    zdb_debug("[+] namespace: reloading: %s\n", namespace->name);

    zdb_debug("[+] namespace: reload: cleaning index\n");
    index_clean_namespace(namespace->index, namespace);

    zdb_debug("[+] namespace: reload: destroying objects\n");
    index_destroy(namespace->index);
    data_destroy(namespace->data);

    zdb_debug("[+] namespace: reload: reloading data\n");
    namespace_load_lazy(nsroot, namespace);

    // hook notification
    namespace_reload_hook(namespace);

    return 0;
}

// start a namespace flushing procees
// when flushing a namespace, we destroy it from
// memory and from disk (except descriptor) then reload (empty) contents
// we don't touch to the namespace object itself (this object is linked to users)
// we only refresh data and index pointers
int namespace_flush(namespace_t *namespace) {
    zdb_debug("[+] namespace: flushing: %s\n", namespace->name);

    zdb_debug("[+] namespace: flushing: cleaning index\n");
    index_clean_namespace(namespace->index, namespace);

    zdb_debug("[+] namespace: flushing: removing files\n");
    index_delete_files(namespace->index);
    data_delete_files(namespace->data);

    zdb_debug("[+] namespace: flushing: destroying objects\n");
    index_destroy(namespace->index);
    data_destroy(namespace->data);

    zdb_debug("[+] namespace: flushing: reloading data\n");
    namespace_load_lazy(nsroot, namespace);

    return 0;
}


static void namespace_delete_hook(namespace_t *namespace) {
    if(!zdb_rootsettings.hook)
        return;

    hook_t *hook = hook_new("namespace-deleted", 2);
    hook_append(hook, zdb_rootsettings.zdbid ? zdb_rootsettings.zdbid : "unknown-id");
    hook_append(hook, namespace->name);
    hook_execute(hook);
    hook_free(hook);
}

//
// delete (clean and remove files) a namespace
//
// note: this function assume namespace exists, you should
// call this by checking before if everything was okay to delete it.
int namespace_delete(namespace_t *namespace) {
    zdb_debug("[+] namespace: removing: %s\n", namespace->name);

    // detach all clients attached to this namespace
    // redis_detach_clients(namespace);

    // unallocating keys attached to this namespace
    index_clean_namespace(namespace->index, namespace);

    // cleaning and closing namespace links
    index_destroy(namespace->index);
    data_destroy(namespace->data);

    // removing namespace slot
    namespace_kick_slot(namespace);

    // removing files
    zdb_debug("[+] namespace: removing: %s\n", namespace->indexpath);
    zdb_dir_remove(namespace->indexpath);

    zdb_debug("[+] namespace: removing: %s\n", namespace->datapath);
    zdb_dir_remove(namespace->datapath);

    // hook notification
    namespace_delete_hook(namespace);

    // unallocating this namespace
    namespace_free(namespace);
    nsroot->effective -= 1;

    return 0;
}


int namespaces_emergency() {
    namespace_t *ns;

    for(ns = namespace_iter(); ns; ns = namespace_iter_next(ns)) {
        printf("[+] namespaces: flushing index [%s]\n", ns->name);

        if(index_emergency(ns->index)) {
            printf("[+] namespaces: flushing data [%s]\n", ns->name);
            // only flusing data if index flush was accepted
            // if index flush returns 0, we are probably in an initializing stage
            data_emergency(ns->data);
        }
    }

    return 0;
}
