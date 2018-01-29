# 0-db
0-db is a simple implementation of a key-value store redis-protocol compatible which
makes data persistant inside an always append index/datafile

# Implementation
This project doesn't rely on any dependencies, it's from scratch.

A rudimental and very simplified redis protocol is supported, allowing only few commands (PING, GET, SET, DEL).

Each index files contains a 26 bytes headers containing a magic 4 bytes identifier,
a version, creation and last opened date and it's own file-sequential-id.

For each entries on the index, 20 bytes plus the key itself (limited to 256 bytes) will be consumed.

The data (value) files contains a 1 dummy byte header and each entries consumes
7 bytes (1 byte for key length, 4 bytes for payload length, 2 bytes crc) plus the key and payload.

> We keep track of the key on the data file in order to be able to rebuild an index based only on datafile if needed.

Each time a key is inserted, an entry is added to the index and to the data. Whenever the key already exists, it's
appended to disk and entry in memory is replaced by the new one.

Each time the server starts, it loads (basicly replay) the index in memory. The index is kept in memory 
**all the time** and only this in-memory index is reached to fetch a key, index files are
never read again except during startup.

When a key-delete is requested, the key is kept in memory and is flagged as deleted. A new entry is added
to the index file, with the according flags. When the server restart, the latest state of the entry is used.

# Index
The current index in memory is a really bad and poor implementation, to be improved.

It uses a rudimental kind-of hashtable. A list of branchs (2^20) is pre-allocated.
Based on the crc32 of the key, we keep 20 bits and uses this as index in the branches.

Branches are allocated only when used. Using 2^20 bits will creates 1 million index entries (8 MB on 64 bits system).
Each branch (when allocated) points to an array of buckets, resized by blocks when growing up.

When the branch is found based on the key, the list of buckets is read sequentialy.

# Supported redis command
- `PING`
- `SET key value`
- `GET key`
- `DEL key`
- `STOP` (used only for debugging, to check memory leaks)

> Compared to real redis protocol, during a `SET`, the key is returned as response.
