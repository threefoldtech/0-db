#ifndef __ZDB_H
    #define __ZDB_H

    #ifndef REVISION
        #define REVISION "(unknown)"
    #endif

    typedef enum db_mode_t {
        // default key-value store
        KEYVALUE = 0,

        // auto-generated sequential id
        SEQUENTIAL = 1,

        // id is hard-fixed data position
        DIRECTKEY = 2,

        // fixed-block length
        DIRECTBLOCK = 3,

        // amount of modes available
        ZDB_MODES

    } db_mode_t;

    // when adding or removing some modes
    // don't forget to adapt correctly the handlers
    // function pointers (basicly for GET and SET)

    typedef struct settings_t {
        char *datapath;   // path where data files will be written
        char *indexpath;  // path where index files will be written
        char *listen;     // network listen address
        int port;         // network listen port
        int verbose;      // enable verbose print (function 'verbose')
        int dump;         // ask to dump index on the load-time
        int sync;         // force to sync each write
        int synctime;     // force to sync writes after this amount of seconds
        db_mode_t mode;   // default index running mode
        char *adminpwd;   // admin password, if NULL, all users are admin
        char *socket;     // unix socket path

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

    } settings_t;

    void hexdump(void *buffer, size_t length);

    #define verbose(...) { if(rootsettings.verbose) { printf(__VA_ARGS__); } }

    #define COLOR_RED    "\033[31;1m"
    #define COLOR_YELLOW "\033[33;1m"
    #define COLOR_GREEN  "\033[32;1m"
    #define COLOR_CYAN   "\033[36;1m"
    #define COLOR_RESET  "\033[0m"

    #define danger(fmt, ...)  { printf(COLOR_RED    fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define warning(fmt, ...) { printf(COLOR_YELLOW fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define success(fmt, ...) { printf(COLOR_GREEN  fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define notice(fmt, ...)  { printf(COLOR_CYAN   fmt COLOR_RESET "\n", ##__VA_ARGS__); }

    #ifndef RELEASE
        #define debug(...) { printf(__VA_ARGS__); }
        #define debughex(...) { hexdump(__VA_ARGS__); }
    #else
        #define debug(...) ((void)0)
        #define debughex(...) ((void)0)
    #endif

    extern settings_t rootsettings;

    void diep(char *str);
    void *warnp(char *str);
#endif
