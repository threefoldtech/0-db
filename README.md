# 0-db
0-db is a simple implementation of a key-value store redis-protocol compatible which
makes data persistant inside an always append index/datafile

# Build targets
Currently supported system:
* Linux (using `epoll`)
* MacOS / FreeBSD (using `kqueue`)

Currently supported hardware:
* Any Intel processor supporting `SSE 4.2`

This project won't compile on something else, for now.

# Build instructions
To build the project (server, tools):
* Type `make` on the root directory
* The binaries will be placed on `bin/` directory

You can build each parts separatly by running `make` in each separated directories.

> By default, the code is compiled in debug mode, in order to use it in production, please use `make release`

# Implementation
This project doesn't rely on any dependencies, it's from scratch.

A rudimental and very simplified redis protocol is supported, allowing only few commands (PING, GET, SET, DEL).

Each index files contains a 26 bytes headers containing a magic 4 bytes identifier,
a version, creation and last opened date and it's own file-sequential-id.

For each entries on the index, 28 bytes (20 bytes + pointer for linked list) 
plus the key itself (limited to 256 bytes) will be consumed.

The data (value) files contains a 1 dummy byte header and each entries consumes
7 bytes (1 byte for key length, 4 bytes for payload length, 2 bytes crc) plus the key and payload.

> We keep track of the key on the data file in order to be able to rebuild an index based only on datafile if needed.

Each time a key is inserted, an entry is added to the data file, then on the index.
Whenever the key already exists, it's appended to disk and entry in memory is replaced by the new one.

Each time the server starts, it loads (basicly replay) the index in memory. The index is kept in memory 
**all the time** and only this in-memory index is reached to fetch a key, index files are
never read again except during startup.

When a key-delete is requested, the key is kept in memory and is flagged as deleted. A new entry is added
to the index file, with the according flags. When the server restart, the latest state of the entry is used.

# Index
The current index in memory is a really simple implementation (to be improved).

It uses a rudimental kind-of hashtable. A list of branchs (2^24) is pre-allocated.
Based on the crc32 of the key, we keep 24 bits and uses this as index in the branches.

Branches are allocated only when used. Using 2^24 bits will creates 16 million index entries
(128 MB on 64 bits system).
Each branch (when allocated) points to a linked-list of keys (collisions).

When the branch is found based on the key, the list is read sequentialy.

# Read-only
You can run 0-db using a read-only filesystem (both for keys or data), which will prevent
any write and let the 0-db serving existing data. This can, in the meantime, allows 0-db
to works on disks which contains failure and would be remounted in read-only by the kernel.

This mode is not possible if you don't have any data/index already available.

# Supported redis command
- `PING`
- `SET key value`
- `GET key`
- `DEL key`
- `STOP` (used only for debugging, to check memory leaks)

`SET`, `GET` and `DEL` supports binary keys.

> Compared to real redis protocol, during a `SET`, the key is returned as response.
