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

// we keep a list of namespaces currently in use
// each namespace used will keep a file descriptor opened
// and only once per namespace
//
// each time a client wants a specific namespace,
// if this namespace is already used by someone else, we give
// the same object back, so they will all share the same
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

// get a namespace from its name
namespace_t *namespace_get(char *name) {
    namespace_t *ns;

    for(ns = namespace_iter(); ns; ns = namespace_iter_next(ns)) {
        if(strcmp(ns->name, name) == 0)
            return ns;
    }

    return NULL;
}

// FIXME: no error handled externally
void namespace_descriptor_update(namespace_t *namespace, int fd) {
    ns_header_t header;

    zdb_debug("[+] namespaces: updating header\n");

    header.version = NAMESPACE_CURRENT_VERSION;
    header.namelength = strlen(namespace->name);
    header.passlength = namespace->password ? strlen(namespace->password) : 0;
    header.maxsize = namespace->maxsize;
    header.flags = NS_FLAGS_EXTENDED;

    if(namespace->public)
        header.flags |= NS_FLAGS_PUBLIC;

    if(namespace->worm)
        header.flags |= NS_FLAGS_WORM;

    if(write(fd, &header, sizeof(ns_header_t)) != sizeof(ns_header_t))
        zdb_warnp("namespace legacy header write");

    if(write(fd, namespace->name, header.namelength) != (ssize_t) header.namelength)
        zdb_warnp("namespace header name write");

    if(namespace->password) {
        if(write(fd, namespace->password, header.passlength) != (ssize_t) header.passlength)
            zdb_warnp("namespace header password write");
    }

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
// a namespace descriptor is a binary file containing the namespace
// specification such as password, maxsize, etc. (see header)
static int namespace_descriptor_load(namespace_t *namespace) {
    ns_header_t header;
    int fd;

    if((fd = namespace_descriptor_open(namespace)) < 0)
        return fd;

    if(read(fd, &header, sizeof(ns_header_t)) != sizeof(ns_header_t)) {
        // probably new file, let's write initial namespace information
        namespace_descriptor_update(namespace, fd);
        close(fd);
        return 0;
    }

    if(header.version != NAMESPACE_CURRENT_VERSION) {
        zdb_danger("[-] %s: unsupported version detected", namespace->name);
        return -1;
    }

    namespace->maxsize = header.maxsize;
    namespace->public = (header.flags & NS_FLAGS_PUBLIC);
    namespace->worm = (header.flags & NS_FLAGS_WORM);
    namespace->version = header.version;

    if(header.passlength) {
        if(!(namespace->password = calloc(sizeof(char), header.passlength + 1))) {
            zdb_warnp("namespace password malloc");
            return 0;
        }

        // skip the namespace name, jump to password
        lseek(fd, sizeof(ns_header_t) + header.namelength, SEEK_SET);

        if(read(fd, namespace->password, header.passlength) != (ssize_t) header.passlength)
            zdb_warnp("namespace password read");
    }

    zdb_log("[+] namespace: [%s] opened, analyzing...\n", namespace->name);
    zdb_debug("[+] -> maxsize: %lu (%.2f MB)\n", namespace->maxsize, MB(namespace->maxsize));
    zdb_debug("[+] -> password protection: %s\n", namespace->password ? "yes" : "no");
    zdb_debug("[+] -> public access: %s\n", namespace->public ? "yes" : "no");
    zdb_debug("[+] -> worm mode: %s\n", namespace->worm ? "yes" : "no");

    close(fd);

    return 1;
}

static void namespace_commit_hook(namespace_t *namespace) {
    if(!zdb_rootsettings.hook)
        return;

    hook_t *hook = hook_new("namespace-updated", 2);
    hook_append(hook, zdb_rootsettings.zdbid ? zdb_rootsettings.zdbid : "unknown-id");
    hook_append(hook, namespace->name);
    hook_execute(hook);
}

// update persistence data of a namespace
// basically, this rewrites it's metadata on disk
int namespace_commit(namespace_t *namespace) {
    int fd;

    if((fd = namespace_descriptor_open(namespace)) < 0)
        return fd;

    // update metadata
    namespace_descriptor_update(namespace, fd);

    close(fd);

    // trigger updated hook
    namespace_commit_hook(namespace);

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
// this just populates data and index from disk
// based on an existing namespace object
// this can be used to load and reload a namespace
static int namespace_load_lazy(ns_root_t *nsroot, namespace_t *namespace) {
    // now, we are sure the namespace exists, but it could be empty
    // let's call index and data initializer, they will take care of that
    namespace->index = index_init(nsroot->settings, namespace->indexpath);
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
    namespace->public = 1;  // by default, namespaces are public (no password)
    namespace->worm = 0;    // by default, worm mode is disabled
    namespace->maxsize = 0; // by default, there are no limits
    namespace->idlist = 0;  // by default, no list is set

    namespace->locked = NS_LOCK_UNLOCKED;           // by default, namespace are unlocked
    namespace->version = NAMESPACE_CURRENT_VERSION; // set current version before reading descriptor

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

    // populating memory
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
}

//
// create (load) a new namespace
//
// note: this function doesn't check if the namespace already exists
// if it already exists, you could end with duplicates in memory
// do not call this is you didn't check if the namespace already exists
// by getting it first
//
int namespace_create(char *name) {
    namespace_t *namespace;

    zdb_log("[+] namespace: creating: %s\n", name);

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
    // it's during init-time; if the directory cannot be read
    // we should not be here at all
    if(!(dp = opendir(root->settings->indexpath)))
        zdb_diep("opendir");

    while((ep = readdir(dp))) {
        if(!namespace_valid_name(ep->d_name))
            continue;

        // skipping non-directory entries
        if(ep->d_type != DT_DIR)
            continue;

        zdb_debug("[+] namespaces: extra found: %s\n", ep->d_name);

        // load the namespace
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
// and keep a reference to the namespace for each entry
//
// because this is the first entry point, this will be the only place where
// we know everything about index and data, so we keep every pointer and allocation
// here, in a global scope
//
// this is why it's here we take care about cleaning and emergencies, it's the only
// place where we __know__ what we needs to clean
ns_root_t *namespaces_allocate(zdb_settings_t *settings) {
    ns_root_t *root;

    // we start with the default namespace
    if(!(root = (ns_root_t *) malloc(sizeof(ns_root_t))))
        zdb_diep("namespaces malloc");

    root->length = 1;             // we start with the default one, only
    root->effective = 1;          // no namespace has been loaded yet
    root->settings = settings;    // keep the reference to the settings, needed for paths

    if(!(root->namespaces = (namespace_t **) calloc(sizeof(namespace_t *), root->length)))
        zdb_diep("namespace malloc");

    return root;
}

static void namespaces_init_hook(zdb_settings_t *settings) {
    if(!settings->hook)
        return;

    hook_t *hook = hook_new("namespaces-init", 3);
    hook_append(hook, settings->zdbid ? settings->zdbid : "unknown-id");
    hook_append(hook, settings->indexpath);
    hook_append(hook, settings->datapath);
    hook_execute_wait(hook);
}

int namespaces_init(zdb_settings_t *settings) {
    zdb_verbose("[+] namespaces: pre-initializing\n");
    namespaces_init_hook(settings);

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
// let's clean all indices, data and namespace arrays
int namespaces_destroy() {
    // calling emergency to ensure we flushed everything
    namespaces_emergency();

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

// return 1 or 0 if namespace is fresh
// a fresh namespace is a completly empty namespace
// without any data/keys, not only an empty namespace
// but even an namespace without any deleted keys
//
// it's important to know if a namespace is 'fresh' in order
// to change it's mode after it's creation, the mode cannot
// be changed if any keys have been written
int namespace_is_fresh(namespace_t *namespace) {
    if(namespace->index->nextentry != 0) {
        zdb_debug("[-] namespace: not fresh: nextentry not zero\n");
        return 0;
    }

    if(namespace->index->nextid != 0) {
        zdb_debug("[-] namespace: not fresh: nextif not zero\n");
        return 0;
    }

    if(namespace->index->indexid != 0) {
        zdb_debug("[-] namespace: not fresh: indexid not zero\n");
        return 0;
    }

    return 1;
}

// trigger hook when namespace is reloaded
static void namespace_reload_hook(namespace_t *namespace) {
    if(!zdb_rootsettings.hook)
        return;

    hook_t *hook = hook_new("namespace-reloaded", 2);
    hook_append(hook, zdb_rootsettings.zdbid ? zdb_rootsettings.zdbid : "unknown-id");
    hook_append(hook, namespace->name);
    hook_execute(hook);
}

// start a namespace reload procees
// when reloading a namespace, we destroy it from
// memory then reload contents, we don't change anything related
// to the namespace object itself (this object is linked to users)
// we only refresh data and index pointers
int namespace_reload(namespace_t *namespace) {
    zdb_debug("[+] namespace: reloading: %s\n", namespace->name);

    zdb_debug("[+] namespace: reload: cleaning index\n");
    index_clean_namespace(namespace->index);

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
    index_clean_namespace(namespace->index);

    char *indexpath = strdup(namespace->index->indexdir);
    char *datapath = strdup(namespace->data->datadir);

    zdb_debug("[+] namespace: flushing: destroying objects\n");
    index_destroy(namespace->index);
    data_destroy(namespace->data);

    zdb_debug("[+] namespace: flushing: removing files\n");
    index_delete_files(indexpath);
    data_delete_files(datapath);

    zdb_debug("[+] namespace: flushing: reloading data\n");
    namespace_load_lazy(nsroot, namespace);

    free(indexpath);
    free(datapath);

    return 0;
}


static void namespace_delete_hook(namespace_t *namespace) {
    if(!zdb_rootsettings.hook)
        return;

    hook_t *hook = hook_new("namespace-deleted", 2);
    hook_append(hook, zdb_rootsettings.zdbid ? zdb_rootsettings.zdbid : "unknown-id");
    hook_append(hook, namespace->name);
    hook_execute(hook);
}

//
// delete (clean and remove files) a namespace
//
// note: this function assume namespace exists, you should
// call this by checking before if everything was okay to delete it.
int namespace_delete(namespace_t *namespace) {
    zdb_log("[+] namespace: removing: %s\n", namespace->name);

    // detach all clients attached to this namespace
    // redis_detach_clients(namespace);

    // unallocating keys attached to this namespace
    index_clean_namespace(namespace->index);

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

static void namespace_flushing_hook(namespace_t *namespace) {
    if(!zdb_rootsettings.hook)
        return;

    // generate dirty list string
    char *dirtylist = index_dirty_list_generate(namespace->index);

    hook_t *hook = hook_new("namespace-closing", 5);
    hook_append(hook, zdb_rootsettings.zdbid ? zdb_rootsettings.zdbid : "unknown-id");
    hook_append(hook, namespace->name);
    hook_append(hook, namespace->index->indexfile);
    hook_append(hook, namespace->data->datafile);
    hook_append(hook, dirtylist ? dirtylist : "");
    hook_execute(hook);

    free(dirtylist);
}

int namespaces_emergency() {
    namespace_t *ns;

    // namespace not allocated yet
    if(namespace_iter() == NULL)
        return 0;

    for(ns = namespace_iter(); ns; ns = namespace_iter_next(ns)) {
        zdb_log("[+] namespaces: flushing: %s\n", ns->name);
        namespace_flushing_hook(ns);

        zdb_debug("[+] namespaces: flushing index [%s]\n", ns->name);

        if(index_emergency(ns->index)) {
            zdb_debug("[+] namespaces: flushing data [%s]\n", ns->name);
            // only flusing data if index flush was accepted
            // if index flush returns 0, we are probably in an initializing stage
            data_emergency(ns->data);
        }
    }

    return 0;
}

// lock a namespace, which set read-only mode for everybody
// this mode is useful when namespace goes in maintenance without
// making namespace unavailable
int namespace_lock(namespace_t *namespace) {
    zdb_debug("[+] namespace: locking namespace: %s\n", namespace->name);
    namespace->locked = NS_LOCK_READ_ONLY;
    return 0;
}

// set namespace back in normal state
int namespace_unlock(namespace_t *namespace) {
    zdb_debug("[+] namespace: unlocking namespace: %s\n", namespace->name);
    namespace->locked = NS_LOCK_UNLOCKED;
    return 0;

}

// check lock status of a namespace (0 is unlocked, 1 is locked or frozen)
int namespace_is_locked(namespace_t *namespace) {
    zdb_debug("[+] namespace: lock status: %s [%d]\n", namespace->name, namespace->locked);
    return (namespace->locked != NS_LOCK_UNLOCKED);
}

// freeze a namespace, which set it unavailable for everybody
// this mode is useful when namespace goes in maintenance for hard changes
// which needs to disable any action on this namespace
int namespace_freeze(namespace_t *namespace) {
    zdb_debug("[+] namespace: freezing namespace: %s\n", namespace->name);
    namespace->locked = NS_LOCK_READ_WRITE;
    return 0;
}

// set namespace back in normal state
int namespace_unfreeze(namespace_t *namespace) {
    zdb_debug("[+] namespace: unfrezzing namespace: %s\n", namespace->name);
    namespace->locked = NS_LOCK_UNLOCKED;
    return 0;

}

// check lock status of a namespace (0 is unlocked, 1 is frozen or locked)
int namespace_is_frozen(namespace_t *namespace) {
    zdb_debug("[+] namespace: lock (freeze) status: %s [%d]\n", namespace->name, namespace->locked);
    return (namespace->locked == NS_LOCK_READ_WRITE);
}


