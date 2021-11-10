#ifndef __ZDB_CRC32_H
    #define __ZDB_CRC32_H

    // return a string with engine in-use for crc32
    char *zdb_crc32_engine_value();

    // libzdb crc32 with auto-selection of best engine
    uint32_t zdb_crc32(const uint8_t *bytes, ssize_t length);

#endif
