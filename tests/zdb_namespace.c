#include <stdio.h>
#include <string.h>
#include "tests_user.h"
#include "zdb_utils.h"
#include "tests.h"

// sequential priority
#define sp 160

static char *namespace_created = "test_ns_create";
static char *namespace_protected = "test_ns_protected";
static char *namespace_password = "helloworld";
static char *namespace_password_try1 = "blabla";
static char *namespace_password_try2 = "hellowo";
static char *namespace_password_try3 = "helloworldhello";

// select not existing namespace
runtest_prio(sp, namespace_select_not_existing) {
    const char *argv[] = {"SELECT", "notfound"};
    return zdb_command_error(test, argvsz(argv), argv);
}

// create a new namespace
runtest_prio(sp, namespace_create) {
    return zdb_nsnew(test, namespace_created);
}

// select this new namespace
runtest_prio(sp, namespace_select_created) {
    const char *argv[] = {"SELECT", namespace_created};
    return zdb_command(test, argvsz(argv), argv);
}

// do a set on this namespace
runtest_prio(sp, namespace_simple_set) {
    return zdb_set(test, "hello", "world");
}

// read the value back
runtest_prio(sp, namespace_simple_get) {
    return zdb_check(test, "hello", "world");
}

// write a new value on this namespace
runtest_prio(sp, namespace_special_set) {
    return zdb_set(test, "special-key", "hello");
}

// read this new value to be sure
runtest_prio(sp, namespace_special_get) {
    return zdb_check(test, "special-key", "hello");
}

// move back to default namespace
runtest_prio(sp, namespace_switchback_default) {
    const char *argv[] = {"SELECT", "default"};
    return zdb_command(test, argvsz(argv), argv);
}

// we should not find "special-key" here (another namespace)
runtest_prio(sp, namespace_default_ensure) {
    const char *argv[] = {"GET", "special-key"};
    return zdb_command_error(test, argvsz(argv), argv);
}

// create a new namespace
runtest_prio(sp, namespace_create_protected) {
    return zdb_nsnew(test, namespace_created);
}

// set password on this namespace
runtest_prio(sp, namespace_set_password) {
    const char *argv[] = {"NSSET", namespace_protected, "password", namespace_password};
    return zdb_command(test, argvsz(argv), argv);
}

// set it as private
runtest_prio(sp, namespace_set_protected) {
    const char *argv[] = {"NSSET", namespace_protected, "public", "0"};
    return zdb_command(test, argvsz(argv), argv);
}

// try to select it with a wrong password
runtest_prio(sp, namespace_select_protected_pass_try1) {
    const char *argv[] = {"SELECT", namespace_protected, namespace_password_try1};
    return zdb_command_error(test, argvsz(argv), argv);
}

// try to select it with a correct prefix-password (see #21)
runtest_prio(sp, namespace_select_protected_pass_try2) {
    const char *argv[] = {"SELECT", namespace_protected, namespace_password_try2};
    return zdb_command_error(test, argvsz(argv), argv);
}

// try to select it with a longer prefix-correct password (see #21)
runtest_prio(sp, namespace_select_protected_pass_try3) {
    const char *argv[] = {"SELECT", namespace_protected, namespace_password_try3};
    return zdb_command_error(test, argvsz(argv), argv);
}

// try to select it without password
runtest_prio(sp, namespace_select_protected_nopass) {
    const char *argv[] = {"SELECT", namespace_protected};
    return zdb_command_error(test, argvsz(argv), argv);
}

// try to select it with the right password
runtest_prio(sp, namespace_select_protected_correct_pass) {
    const char *argv[] = {"SELECT", namespace_protected, namespace_password};
    return zdb_command(test, argvsz(argv), argv);
}

// go back to default, again
runtest_prio(sp, namespace_switchback_default_2) {
    const char *argv[] = {"SELECT", "default"};
    return zdb_command(test, argvsz(argv), argv);
}

// try to switch to the same namespace we currently are
runtest_prio(sp, namespace_switchback_default_again) {
    const char *argv[] = {"SELECT", "default"};
    return zdb_command(test, argvsz(argv), argv);
}


