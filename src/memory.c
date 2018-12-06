#include <stdio.h>
#include <stdlib.h>
#include <execinfo.h>
#include <stdint.h>
#include "zerodb.h"

// pre-allocated to ensure we can use it
static void *backtrace_buffer[1024];

void emergency_backtrace() {
    fprintf(stderr, "[-] ----------------------------------\n");

    int calls = backtrace(backtrace_buffer, sizeof(backtrace_buffer) / sizeof(void *));
    backtrace_symbols_fd(backtrace_buffer, calls, 1);

    fprintf(stderr, "[-] ----------------------------------\n");
}

// theses memory allocation wrapper are used to improve
// tests and coverage, to ensure that if some memory allocation fails
// we can't take the right decision

// a malloc fatal is a memory allocation from which we can't
// recovery, we can't continue to live without this (required allocation)
//
// eg: we can't allocate zerodb server id, we can't live without id
void *malloc_fatal(size_t length) {
    void *ptr;

    #ifdef MEMORY_ERROR_SIMULATOR
    if((rand() % 100) < MEMORY_ERROR_PROBABILITY) {
        // try to allocate a too huge segment
        length = 0xffffffffffff;
    }
    #endif

    if(!(ptr = malloc(length))) {
        emergency_backtrace();
        diep("malloc");
    }

    return ptr;
}

// a malloc survivable is a memory allocation from which we can
// still live but in a degraded mode or in a temporary unstable state
//
// eg: we can't allocate memory to reply to a GET, that's doesn't mean we can't
//    still live to provide a reply to another one later
void *malloc_survive(size_t length) {
    void *ptr;

    #ifdef MEMORY_ERROR_SIMULATOR
    if((rand() % 100) < MEMORY_ERROR_PROBABILITY) {
        // try to allocate a too huge segment
        length = 0xffffffffffff;
    }
    #endif

    if(!(ptr = malloc(length))) {
        emergency_backtrace();
        warnp("malloc");
    }

    return ptr;
}
