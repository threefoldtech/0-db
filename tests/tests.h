#ifndef ZDB_TESTS_H
    #define ZDB_TESTS_H

    typedef struct runtest_t {
        char *name;             // function name
        int (*test)(test_t *);  // function pointer
        int result;             // return code

    } runtest_t;

    typedef struct registered_tests_t {
        unsigned int length;
        unsigned int longest;
        runtest_t list[1024];

        unsigned int success;
        unsigned int failed;
        unsigned int failed_fatal;
        unsigned int warning;

    } registered_tests_t;

    void tests_register(char *name, int (*func)(test_t *));

    // tests return code
    #define TEST_SUCCESS       (0)
    #define TEST_FAILED_FATAL  (-1)
    #define TEST_FAILED        (-2)
    #define TEST_WARNING       (-3)
    #define TEST_SKIPPED       (-4)

    // test logger
    #define log(...)  { printf("[ ]    " __VA_ARGS__); }

    #define __executor(name) name
    #define __constructor(name) __construct_##name

    // macro definition:
    //
    // int name(test_t *test);                                      -> declare prototype
    // __attribute__ ((constructor)) void __constructor(name)() {   -> create a constructor
    //     tests_register(#name, name);                             -> call the global register
    // }                                                            -> end of the register
    // int __executor(name)(test_t *test)                           -> declare the real function

    #define runtest_prio(prio, name) \
            int name(test_t *test); \
            __attribute__ ((constructor (prio))) void __constructor(name)() { \
                tests_register(#name, name); \
            } \
            int __executor(name)(test_t *test)

    #define runtest(name) runtest_prio(1000, name)

    //
    // here is a more readable expanded version
    //
    // -----------------------------------
    // runtest(test1) {
    //    printf(">> I'm test 1\n");
    //    return 0;
    // }
    // -----------------------------------
    //
    //  -> is translated to:
    //
    // -----------------------------------
    // int test1(test_t *test);
    //
    // __attribute__ ((constructor)) void __constructor_test1() {
    //     tests_register("test1", test1);
    // }
    //
    // int test1(test_t *test) {
    //     printf(">> I'm test 1\n");
    //     return 0;
    // }
    // -----------------------------------
    //

    #define COLOR_RED    "\033[31;1m"
    #define COLOR_YELLOW "\033[33;1m"
    #define COLOR_GREEN  "\033[32;1m"
    #define COLOR_CYAN   "\033[36;1m"
    #define COLOR_GREY   "\033[30;1m"
    #define COLOR_RESET  "\033[0m"

    #define RED(x)    COLOR_RED x COLOR_RESET
    #define YELLOW(x) COLOR_YELLOW x COLOR_RESET
    #define GREEN(x)  COLOR_GREEN x COLOR_RESET
    #define CYAN(x)   COLOR_CYAN x COLOR_RESET
    #define GREY(x)   COLOR_GREY x COLOR_RESET

    void initialize();
    int initialize_tcp();
#endif
