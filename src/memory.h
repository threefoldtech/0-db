#ifndef ZDB_MEMORY_H
    #define ZDB_MEMORY_H

    void emergency_backtrace();

    void *malloc_fatal(size_t length);
    void *malloc_survive(size_t length);
#endif
