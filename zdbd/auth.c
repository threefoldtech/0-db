#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libzdb.h"
#include "zdbd.h"

// check if user input (buffer with buffer length)
// does match with expected password string, unified way
int zdbd_password_check(char *input, int length, char *expected) {
    char password[192];
    sprintf(password, "%.*s", length, input);

    if(strcmp(password, expected) == 0) {
        zdbd_debug("[+] password: access granted\n");
        return 1;
    }

    zdbd_debug("[-] password: wrong password\n");
    return 0;
}


