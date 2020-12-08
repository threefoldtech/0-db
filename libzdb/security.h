#ifndef __ZDB_SECURITY_H
    #define __ZDB_SECURITY_H

    char *zdb_challenge();
    char *zdb_hash_password(char *salt, char *password);
#endif
