# 0-db
0-db is a simple implementation of a key-value store redis-protocol compatible which
makes data persistant behing always append index/datafile

# Implementation
This project rely on any dependencies, it's from scratch.

A rudimental and very simplified redis protocol is supported, allowing only few commands (PING, GET, SET).

Each index files contains a 26 bytes headers containing a magic 4 bytes identifier,
a version, creation and last opened date and it's own file-sequential-id.

For each entries on the index, 20 bytes plus the key size (limited to 256 bytes) will be consumed.

The data (value) files contains a 1 dummy byte header and each entries consumes
5 bytes plus the key and payload size.

> We keep track of the key on the data file in order to be able to rebuild an index based only on datafile if needed.

Each time a key is inserted, an entry is added to the index and to the data. Whenever the key already exists, it's
appended to disk and entry in memory is replaced by the new one.

Each time the server starts, it loads (basicly replay) the index in memory. The index is **always** in memory and
only this in-memory index is reached to fetch a key, index files are never read except during startup.

# Supported redis command
- `PING`
- `SET key value`
- `GET key`

> Compared to real redis protocol, during a `SET`, the key is returned as response.
