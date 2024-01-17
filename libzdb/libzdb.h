#ifndef __LIBZDB_H
    #define __LIBZDB_H

    #include <stdint.h>
    #include <time.h>
    #include <sys/time.h>
    #include "hook.h"

    #ifndef ZDB_REVISION
        #define ZDB_REVISION "(unknown)"
    #endif

    #define ZDB_DEFAULT_DATAPATH    "./zdb-data"
    #define ZDB_DEFAULT_INDEXPATH   "./zdb-index"

    #define ZDB_PATH_MAX    4096

    // define the version of datafile and indexfile
    // theses versions are written in a header of each created file
    //
    // the version will only change (increment of 1) if the
    // format of the binary struct changes
    //
    // one version of 0-db will only support one version of filelayout
    // there is no backward compatibility planned
    //
    // if some updates are made, upgrade tools should be written
    // to update the existing databases
    #define ZDB_DATAFILE_VERSION    3
    #define ZDB_IDXFILE_VERSION     4

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
    #define ZDB_VERSION     "2.0.6"

    typedef struct zdb_stats_t {
        struct timeval inittime;  // timestamp when zdb started (used for uptime)

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

        uint32_t childwait;       // amount of hook child pending

    } zdb_stats_t;

    typedef struct zdb_settings_t {
        char *datapath;    // path where data files will be written
        char *indexpath;   // path where index files will be written
        int verbose;       // enable verbose print (function 'verbose')
        int dump;          // ask to dump index on the load-time
        int sync;          // force to sync each write
        int synctime;      // force to sync writes after this period (in seconds)
        int mode;          // default index running mode (should be index_mode_t)
        char *hook;        // external hook script to execute
        size_t datasize;   // maximum datafile size before jumping to next one
        size_t maxsize;    // default namespace maximum datasize
        int initialized;   // single instance lock flag

        int secure;        // enable some security about data write, but will
                           // reduce performance (eg: will fsync() before jumping
                           // to another file, to ensure file is written, but on
                           // write burst, this can dramatically reduce performance

        // right now, the library can't handle multiple instance on the
        // same time, there is a global zdb_settings variable shared with
        // the global program
        //
        // this needs to be fixed later

        char *zdbid;         // fake 0-db id generated based on listening
        uint32_t iid;        // 0-db random instance id generated on boot

        zdb_stats_t stats;   // global 0-db statistics
        zdb_hooks_t hooks;   // global hooks running list

        int indexlock;       // index path lock file descriptor
        int datalock;        // data path lock file descriptor

        // the synctime can be useful to add basic security without killing
        // performance
        //
        // if low amount of writes are performed, the kernel will probably
        // flush changes to the disk quickly after the write
        // if there are a lot of subsequent writes, the kernel can take a long time
        // (couple of seconds) before writing anything on disk
        //
        // doing each write sync is, of course, the most secure way to ensure
        // everything is persistent and well written to disk, but this is a real
        // performance killer
        // forcing data to be written every, let's says, 5 seconds
        // allows you to enfore during heavy load that you could lose at maximum
        // 5 seconds of data and not more (if a write has been made after the last flush)
        //
        // if no write is made before the timeout, the kernel probably did the write itself
        //
        // WARNING: all of this really relies on the fact that the disk is not used
        //          at all by something else on the system; if another process uses
        //          the disk, the disk will not be available to be synced automatically
        //
        //          this option is really a life-saver on heavy write loaded system
        //          and try to do some mix between efficiency and security but this
        //          is NOT a way you can entierly trust

    } zdb_settings_t;

    // fileid_t represent internal size of fileid type, which was
    // historically hardcoded to uint16_t everywhere, but make code less
    // easy to update if this size need to change
    //
    // now this field is fixed to 32 bits, which allows 32 PB of data even
    // with 8 MB datafile size
    typedef uint32_t fileid_t;

    void zdb_tools_fulldump(void *_data, size_t len);
    void zdb_tools_hexdump(void *input, size_t length);
    char *zdb_header_date(uint32_t epoch, char *target, size_t length);
    size_t *zdb_human_readable_parse(char *input, size_t *target);

    void zdb_timelog(FILE *fp);
    void *zdb_warnp(char *str);
    void zdb_diep(char *str);

    #define COLOR_RED    "\033[31;1m"
    #define COLOR_YELLOW "\033[33;1m"
    #define COLOR_GREEN  "\033[32;1m"
    #define COLOR_CYAN   "\033[36;1m"
    #define COLOR_RESET  "\033[0m"

    #define zdb_log(fmt, ...)     { zdb_timelog(stdout); printf(fmt, ##__VA_ARGS__); }
    #define zdb_logerr(fmt, ...)  { zdb_timelog(stderr); fprintf(stderr, fmt, ##__VA_ARGS__); }

    #define zdb_danger(fmt, ...)  { zdb_timelog(stdout); printf(COLOR_RED    fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdb_warning(fmt, ...) { zdb_timelog(stdout); printf(COLOR_YELLOW fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdb_success(fmt, ...) { zdb_timelog(stdout); printf(COLOR_GREEN  fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdb_notice(fmt, ...)  { zdb_timelog(stdout); printf(COLOR_CYAN   fmt COLOR_RESET "\n", ##__VA_ARGS__); }

    #define KB(x)   (x / (1024.0))
    #define MB(x)   (x / (1024 * 1024.0))
    #define GB(x)   (x / (1024 * 1024 * 1024.0))
    #define TB(x)   (x / (1024 * 1024 * 1024 * 1024.0))

    #include "data.h"
    #include "crc32.h"
    #include "filesystem.h"
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
    #include "sha1.h"
    #include "security.h"
    #include "api.h"

    // stop compilation on big-endian platform, which is not
    // supported yet
    //
    // this test is not fully accurate but this just prevent some
    // unwanted compilation on known failure system
    #if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
        #error "Big Endian system unsupported"
    #endif
#endif
