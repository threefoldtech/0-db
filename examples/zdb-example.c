#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "libzdb.h"

void dump_type(zdb_api_type_t status) {
    char *msg = zdb_api_debug_type(status);
    printf("[+] example: " COLOR_CYAN "%s" COLOR_RESET "\n", msg);
}

int stuff(zdb_settings_t *settings) {
    (void) settings;
    namespace_t *ns = namespace_get_default();

    //
    // set a key
    //
    char *key = "MyKey";
    char *data = "Hello World !";

    zdb_api_t *reply = zdb_api_set(ns, key, strlen(key), data, strlen(data));
    dump_type(reply->status);
    zdb_api_reply_free(reply);

    //
    // ensure existance
    //
    printf("[+] example: checking if key exists\n");
    reply = zdb_api_exists(ns, key, strlen(key));
    dump_type(reply->status);
    zdb_api_reply_free(reply);

    //
    // looking up for this entry
    //
    reply = zdb_api_get(ns, key, strlen(key));
    dump_type(reply->status);

    if(reply->status == ZDB_API_ENTRY) {
        zdb_api_entry_t *entry = reply->payload;
        printf("[+] entry: key: <%.*s>\n", (int) entry->key.size, entry->key.payload);
        printf("[+] entry: data: <%.*s>\n", (int) entry->payload.size, entry->payload.payload);
    }

    zdb_api_reply_free(reply);

    //
    // checking consistancy
    //
    printf("[+] example: checking data consistancy\n");
    reply = zdb_api_check(ns, key, strlen(key));
    dump_type(reply->status);
    zdb_api_reply_free(reply);

    //
    // deleting the key
    //
    printf("[+] example: deleting key\n");
    reply = zdb_api_del(ns, key, strlen(key));
    dump_type(reply->status);
    zdb_api_reply_free(reply);

    //
    // checking if key was well removed
    //
    printf("[+] example: checking for key suppression\n");
    reply = zdb_api_exists(ns, key, strlen(key));
    dump_type(reply->status);
    zdb_api_reply_free(reply);



    return 0;
}

// int main(int argc, char *argv[]) {
int main() {
    printf("[*] 0-db example small example code\n");
    printf("[*] 0-db engine v%s\n", zdb_version());
    printf("[*] 0-db engine revision: %s\n", zdb_revision());

    // initialize default settings
    zdb_settings_t *zdb_settings = zdb_initialize();

    // set a dump id, you should always set an id, if possible
    // unique, this id is mainly used to differenciate running
    // zdb when using hook system
    zdb_id_set("example-run");

    // each zdb have an instance id generated randomly
    // when initialized
    printf("[+] instance id: %u\n", zdb_instanceid_get());

    // set custom data and index path for our database
    // keep all others settings by default
    zdb_settings->datapath = "/tmp/zdb-example/data";
    zdb_settings->indexpath = "/tmp/zdb-example/index";

    // open the database
    zdb_open(zdb_settings);

    // ---------------------
    stuff(zdb_settings);
    // ---------------------

    // closing and memory freeing everything
    // related to database stuff
    zdb_close(zdb_settings);

    return 0;
}
