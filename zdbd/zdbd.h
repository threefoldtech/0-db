#ifndef __ZDBD_H
    #define __ZDBD_H

    #ifndef ZDBD_REVISION
        #define ZDBD_REVISION "(unknown)"
    #endif

    #define ZDBD_DEFAULT_LISTENADDR  "0.0.0.0"
    #define ZDBD_DEFAULT_PORT        9900

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
    #define ZDBD_VERSION     "1.0.0"

    typedef struct zdbd_stats_t {
        time_t boottime;          // timestamp when zdb started (used for uptime)
        uint32_t clients;         // lifetime amount of clients connected

        // commands
        uint64_t cmdsvalid;       // amount of commands (found) executed
        uint64_t cmdsfailed;      // amount of commands nof found received
        uint64_t adminfailed;     // amount of authentication failed

        // network
        uint64_t networkrx;       // amount of bytes received over the network
        uint64_t networktx;       // amount of bytes transmitted over the network

    } zdbd_stats_t;

    typedef struct zdbd_settings_t {
        char *listen;     // network listen address
        int port;         // network listen port
        int verbose;      // enable verbose print (function 'verbose')
        char *adminpwd;   // admin password, if NULL, all users are admin
        char *socket;     // unix socket path
        int background;   // flag to run in background
        char *logfile;    // where to redirect logs in background mode
        int protect;      // flag default namespace to use admin password (for writing)

        zdbd_stats_t stats;

    } zdbd_settings_t;

    void zdbd_hexdump(void *buffer, size_t length);
    void zdbd_fulldump(void *data, size_t len);

    #define zdbd_danger(fmt, ...)  { printf(COLOR_RED    fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdbd_warning(fmt, ...) { printf(COLOR_YELLOW fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdbd_success(fmt, ...) { printf(COLOR_GREEN  fmt COLOR_RESET "\n", ##__VA_ARGS__); }
    #define zdbd_notice(fmt, ...)  { printf(COLOR_CYAN   fmt COLOR_RESET "\n", ##__VA_ARGS__); }

    #ifndef RELEASE
        #define zdbd_verbose(...) { printf(__VA_ARGS__); }
        #define zdbd_debug(...) { printf(__VA_ARGS__); }
        #define zdbd_debughex(...) { zdbd_hexdump(__VA_ARGS__); }
    #else
        #define zdbd_verbose(...) { if(zdbd_rootsettings.verbose) { printf(__VA_ARGS__); } }
        #define zdbd_debug(...) ((void)0)
        #define zdbd_debughex(...) ((void)0)
    #endif

    extern zdbd_settings_t zdbd_rootsettings;

    void zdbd_diep(char *str);
    void *zdbd_warnp(char *str);
    void zdbd_verbosep(char *prefix, char *str);
#endif
