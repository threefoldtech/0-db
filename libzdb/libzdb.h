#ifndef __LIBZDB_H
    #define __LIBZDB_H

    #include <stdint.h>
    #include <time.h>

    #ifndef ZDB_REVISION
        #define ZDB_REVISION "(unknown)"
    #endif

    #define ZDB_DEFAULT_DATAPATH    "./zdb-data"
    #define ZDB_DEFAULT_INDEXPATH   "./zdb-index"

    #define ZDB_PATH_MAX    4096

    // define here version of datafile and indexfile
    // theses version are written on header of each file created
    //
    // the version will only change (increment of 1) if the
    // format of the binary struct change
    //
    // one version of 0-db will only support one version of file
    // there no backward compatibility planed
    //
    // if some update are made, upgrade tools could be written
    // to update existing database
    #define ZDB_DATAFILE_VERSION    2
    #define ZDB_IDXFILE_VERSION     2

    // define here version of 0-db itself
    // version is made as following:
    //
    //   Major.Minor.Review
    //
    //               ^-- incremented on bug fix and small changes
    //
    //         ^-- incremented on important new features
    //
    //   ^--- will only change if data format change
    //        and not assure retro-compatibility
    //        (eg: files written on version 1.x.x won't works
    //             out of box on a version 2.x.x)
    #define ZDB_VERSION     "1.1.0"

    typedef struct zdb_stats_t {
        time_t inittime;          // timestamp when zdb started (used for uptime)

        // index
        uint64_t idxreadfailed;   // amount of index disk read failure
        uint64_t idxwritefailed;  // amount of index disk write failure
        uint64_t idxdiskread;     // amount of index bytes read on disk (except index loader)
        uint64_t idxdiskwrite;    // amount of index bytes written on disk (except namespace creation)

        // data
        uint64_t datareadfailed;  // amount of data payload disk read failure
        uint64_t datawritefailed; // amount of data payload disk write failure
        uint64_t datadiskread;    // amount of data bytes read on disk (except index loader)
        uint64_t datadiskwrite;   // amount of data bytes written on disk (except namespace creation)

    } zdb_stats_t;

    typedef struct zdb_settings_t {
        char *datapath;    // path where data files will be written
        char *indexpath;   // path where index files will be written
        int verbose;       // enable verbose print (function 'verbose')
        int dump;          // ask to dump index on the load-time
        int sync;          // force to sync each write
        int synctime;      // force to sync writes after this amount of seconds
        int mode;          // default index running mode (should be index_mode_t)
        char *hook;        // external hook script to execute
        size_t datasize;   // maximum datafile size before jumping to next one
        size_t maxsize;    // default namespace maximum datasize

        char *zdbid;      // fake 0-db id generated based on listening
        uint32_t iid;     // 0-db random instance id generated on boot

        zdb_stats_t stats; // global 0-db statistics

        // the synctime can be useful to add basic security without killing
        // performance
        //
        // if low amount of write are performed, the kernel will probably
        // flush changes to the disk quickly after the write
        // if a lot of write are subsequents, the kernel can takes a long time
        // (couple of seconds) before writing anything on disk
        //
        // doing each write sync if, of course, the most secure way to ensure
        // everything is persistant and well written, but this is a really
        // performance killer, forcing data to be written each, let's says, 5 seconds
        // allows you to encore on heavy loads that you could lost at least maximum
        // 5 seconds of data and not more (if a write has been made after)
        //
        // if no write were made in the timeout, the kernel probably did the write
        // in the meantime because the disk was not busy
        //
        // WARNING: all of this really rely on the fact that the disk is not used
        //          at all by something else on the system, if another process use
        //          the disk, the disk will not be available to be sync'd automatically
        //
        //          this option is really a life-saver on heavy write loaded system
        //          and try to do some mix between efficienty and security but this
        //          is NOT a way you can entierly trust

    } zdb_settings_t;

    void zdb_tools_fulldump(void *_data, size_t len);
    void zdb_tools_hexdump(void *input, size_t length);
    char *zdb_header_date(uint32_t epoch, char *target, size_t length);

    void *zdb_warnp(char *str);
    void zdb_diep(char *str);

    #define COLOR_RED    "\033[31;1m"
    #define COLOR_YELLOW "\033[33;1m"
    #define COLOR_GREEN  "\033[32;1m"
    #define COLOR_CYAN   "\033[36;1m"
    #define COLOR_RESET  "\033[0m"

    #define zdb_danger(fmt, ...)  { printf(COLOR_RED    fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdb_warning(fmt, ...) { printf(COLOR_YELLOW fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdb_success(fmt, ...) { printf(COLOR_GREEN  fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdb_notice(fmt, ...)  { printf(COLOR_CYAN   fmt COLOR_RESET "\n", ##__VA_ARGS__); }

    #define KB(x)   (x / (1024.0))
    #define MB(x)   (x / (1024 * 1024.0))
    #define GB(x)   (x / (1024 * 1024 * 1024.0))
    #define TB(x)   (x / (1024 * 1024 * 1024 * 1024.0))

    #include "data.h"
    #include "filesystem.h"
    #include "hook.h"
    #include "index.h"
    #include "index_branch.h"
    #include "index_get.h"
    #include "index_loader.h"
    #include "index_scan.h"
    #include "index_seq.h"
    #include "index_set.h"
    #include "namespace.h"
    #include "settings.h"
    #include "bootstrap.h"
    #include "api.h"
#endif
