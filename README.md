# 0-db [![Build Status](https://travis-ci.org/rivine/0-db.svg?branch=master)](https://travis-ci.org/zero-os/0-db)
0-db is a super fast and efficient key-value store redis-protocol (mostly) compatible which
makes data persistant inside an always append index/datafile, with namespaces support.

We use it as backend for many of our blockchain work and might replace redis for basic
SET and GET request.

# Build targets
Currently supported system:
* Linux (using `epoll`)
* MacOS / FreeBSD (using `kqueue`)

Currently supported hardware:
* Any Intel processor supporting `SSE 4.2`

This project won't compile on something else (for now).

# Build instructions
To build the project (server, tools):
* Type `make` on the root directory
* The binaries will be placed on `bin/` directory

You can build each parts separatly by running `make` in each separated directories.

> By default, the code is compiled in debug mode, in order to use it in production, please use `make release`

# Running modes
On the runtime, you can choose between multiple mode:
* `user`: user-key mode
* `seq`: sequential mode
* `direct`: direct-position key mode

## User Key
This is a default mode, a simple key-value store. User can `SET` their own keys, like any key-value store.

Even in this mode, the key itself is returned by the `SET` command.

## Sequential
In this mode, the key is a sequential key autoincremented.

You need to provide a null-length key, to generate a new key.
If you provide a key, this key should exists (a valid generated key), and the `SET` will update that key.

Providing any other key will fails.

The id is a little-indian integer key. All the keys are kept in memory.

## Direct Key
This mode works like the sequential mode, except that returned key contains enough information to fetch the
data back, without using any index. This mode doesn't use index in memory.

There is no update possible in this mode (since the key itself contains data to the real location
and we use always append method, we can't update existing data). Providing a key has no effect and
is ignored.

The key returned by the `SET` command is a binary key.

# Implementation
This project doesn't rely on any dependencies, it's from scratch.

A rudimental and very simplified redis protocol is supported, allowing only few commands. See below.

Each index files contains a 26 bytes headers containing a magic 4 bytes identifier,
a version, creation and last opened date and it's own file-sequential-id.

For each entries on the index, 28 bytes (20 bytes + pointer for linked list) 
plus the key itself (limited to 256 bytes) will be consumed.

The data (value) files contains a 26 bytes headers, mostly the same as the index one
and each entries consumes 7 bytes (1 byte for key length, 4 bytes for payload length, 2 bytes crc)
plus the key and payload.

> We keep track of the key on the data file in order to be able to rebuild an index based only on datafile if needed.

Each time a key is inserted, an entry is added to the data file, then on the index.
Whenever the key already exists, it's appended to disk and entry in memory is replaced by the new one.

Each time the server starts, it loads (basicly replay) the index in memory. The index is kept in memory 
**all the time** and only this in-memory index is reached to fetch a key, index files are
never read again except during startup (except for 'direct-key mode', where the index is not in memory).

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
- `EXISTS key`
- `CHECK key`
- `INFO`
- `NSNEW`
- `NSINFO`
- `NSLIST`
- `NSSET`
- `SELECT`
- `DBSIZE`
- `TIME`
- `AUTH`

`SET`, `GET` and `DEL` supports binary keys.

> Compared to real redis protocol, during a `SET`, the key is returned as response.

## EXISTS
Returns 1 or 0 if the key exists

## CHECK
Check internally if the data is corrupted or not. A CRC check is done internally.
Returns 1 if integrity is validated, 0 otherwise.

## NSNEW
Create a new namespace. Only admin can do this.

By default, a namespace is not password protected, is public and not size limited.

## NSINFO
Returns basic informations about a namespace

## NSLIST
Returns an array of all available namespaces.

## NSSET
Change a namespace setting/property. Only admin can do this.

Properties:
* `maxsize`: set the maximum size in bytes, of the namespace's data set
* `password`: lock the namespace by a password, use `*` password to clear it
* `public`: change the public flag, a public namespace can be read-only if a password is set

## SELECT
Change your current namespace. If the requested namespace is password-protected, you need
to add the password as extra parameter. If the namespace is `public` and you don't provide
any password, the namespace will be accessible in read-only.

## AUTH
If an admin account is set, use `AUTH` command to authentificate yourself as `ADMIN`.

# Namespace (simple demo)
New commands are implemented to support **Namespaces** notion.

Each namespace can be optionally protected by a password. A namespace is an isolated key-space.

You are always attached to a namespace, by default, it's namespace `default`.

Quick demo and exemple how to use namespaces:
```
127.0.0.1:9900> NSNEW demo
OK

127.0.0.1:9900> SET hello foo
"hello"

127.0.0.1:9900> GET hello
"foo"

[... switching namespace ...]

127.0.0.1:9900> SELECT demo
OK

127.0.0.1:9900> SET hello bar
"hello"

127.0.0.1:9900> GET hello
"bar"

[... rolling back to first namespace ...]

127.0.0.1:9900> SELECT default
OK

127.0.0.1:9900> GET hello
"foo"

```
