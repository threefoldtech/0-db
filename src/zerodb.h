#ifndef __ZDB_H
    #define __ZDB_H

    typedef struct settings_t {
        char *datapath;
        char *indexpath;
        char *listen;
        int port;
        int verbose;
        int dump;

    } settings_t;

    #define verbose(...) { if(rootsettings.verbose) { printf(__VA_ARGS__); } }

    #ifndef RELEASE
        #define debug(...) { printf(__VA_ARGS__); }
    #else
        #define debug(...) ((void)0)
    #endif

    extern settings_t rootsettings;

    void diep(char *str);
    void warnp(char *str);
#endif
