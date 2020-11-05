#ifndef ZDBD_AUTH_H
    #define ZDBD_AUTH_H

    int zdbd_password_check(char *input, int length, char *expected);
    char *zdb_hash_password(char *salt, char *password);
#endif
