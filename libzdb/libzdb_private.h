#ifndef __LIBZDB_PRIVATE_H
    #define __LIBZDB_PRIVATE_H

    void zdb_hexdump(void *buffer, size_t length);
    void zdb_fulldump(void *data, size_t len);

    #ifndef RELEASE
        #define zdb_verbose(...) { printf(__VA_ARGS__); }
        #define zdb_debug(...) { printf(__VA_ARGS__); }
        #define zdb_debughex(...) { zdb_hexdump(__VA_ARGS__); }
    #else
        #define zdb_verbose(...) { if(zdb_rootsettings.verbose) { printf(__VA_ARGS__); } }
        #define zdb_debug(...) ((void)0)
        #define zdb_debughex(...) ((void)0)
    #endif

    extern zdb_settings_t zdb_rootsettings;

    void zdb_diep(char *str);
    void *zdb_warnp(char *str);
    void zdb_verbosep(char *prefix, char *str);
#endif
