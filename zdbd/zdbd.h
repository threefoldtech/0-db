#ifndef __ZDBD_H
    #define __ZDBD_H

    #ifndef ZDBD_REVISION
        #define ZDBD_REVISION "(unknown)"
    #endif

    #define ZDBD_DEFAULT_LISTENADDR  "::"
    #define ZDBD_DEFAULT_PORT        "9900"

    #define ZDBD_PATH_MAX    4096

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
    #define ZDBD_VERSION     "2.0.0-rc6"

    typedef struct zdbd_stats_t {
        // boottime is kept for zdbd uptime statistics (for INFO command)
        // but we use the libzdb inittime for logs
        struct timeval boottime;  // timestamp when zdb started
        uint32_t clients;         // lifetime amount of clients connected

        // commands
        uint64_t cmdsvalid;       // amount of commands (found) executed
        uint64_t cmdsfailed;      // amount of commands nof found received
        uint64_t adminfailed;     // amount of authentication failed

        // network
        uint64_t networkrx;       // amount of bytes received over the network
        uint64_t networktx;       // amount of bytes transmitted over the network
        uint64_t netevents;       // amount of socket events received

    } zdbd_stats_t;

    typedef struct zdbd_settings_t {
        char *listen;     // network listen address
        char *port;       // network listen port
        int verbose;      // enable verbose print (function 'verbose')
        char *adminpwd;   // admin password, if NULL, all users are admin
        char *socket;     // unix socket path
        int background;   // flag to run in background
        char *logfile;    // where to redirect logs in background mode
        int protect;      // flag default namespace to use admin password (for writing)
        int dualnet;      // support for dual socket listening
        int rotatesec;    // amount of seconds before forcing rotation of index/data

        zdbd_stats_t stats;

    } zdbd_settings_t;

    void zdbd_hexdump(void *buffer, size_t length);
    void zdbd_fulldump(void *data, size_t len);

    #define zdbd_log(fmt, ...)     { zdbd_timelog(stdout); printf(fmt, ##__VA_ARGS__); }
    #define zdbd_logerr(fmt, ...)  { zdbd_timelog(stderr); fprintf(stderr, fmt, ##__VA_ARGS__); }

    #define zdbd_danger(fmt, ...)  { zdbd_timelog(stdout); printf(COLOR_RED    fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdbd_warning(fmt, ...) { zdbd_timelog(stdout); printf(COLOR_YELLOW fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdbd_success(fmt, ...) { zdbd_timelog(stdout); printf(COLOR_GREEN  fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdbd_notice(fmt, ...)  { zdbd_timelog(stdout); printf(COLOR_CYAN   fmt COLOR_RESET "\n", ##__VA_ARGS__); }

    #ifndef RELEASE
        #define zdbd_verbose(...)  { zdbd_timelog(stdout); printf(__VA_ARGS__); }
        #define zdbd_debug(...)    { zdbd_timelog(stdout); printf(__VA_ARGS__); }
        #define zdbd_debughex(...) { zdbd_hexdump(__VA_ARGS__); }
    #else
        #define zdbd_verbose(...) { if(zdbd_rootsettings.verbose) { zdbd_timelog(stdout); printf(__VA_ARGS__); } }
        #define zdbd_debug(...) ((void)0)
        #define zdbd_debughex(...) ((void)0)
    #endif

    extern zdbd_settings_t zdbd_rootsettings;

    void zdbd_timelog(FILE *fp);
    void zdbd_diep(char *str);
    void zdbd_dieg(char *str, int status);
    void *zdbd_warnp(char *str);
    void zdbd_verbosep(char *prefix, char *str);

    #ifdef __APPLE__
        #define	bswap_16(value) ((((value) & 0xff) << 8) | ((value) >> 8))
        #define	bswap_32(value)	(((uint32_t) bswap_16((uint16_t)((value) & 0xffff)) << 16) | (uint32_t) bswap_16((uint16_t)((value) >> 16)))
        #define	bswap_64(value)	(((uint64_t) bswap_32((uint32_t)((value) & 0xffffffff)) << 32) | (uint64_t) bswap_32((uint32_t)((value) >> 32)))
    #else
        #include <byteswap.h>
    #endif
#endif
