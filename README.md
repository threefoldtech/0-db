# 0-db [![codecov](https://codecov.io/gh/threefoldtech/0-db/branch/master/graph/badge.svg)](https://codecov.io/gh/threefoldtech/0-db)  
0-db (zdb) is a super fast and efficient key-value store which makes data persistant
inside an always append datafile, with namespaces support and data offloading.

The database is split in two part:
- The database engine, which can be used as static or shared library
- The network daemon using resp (redis protocol) to make a remote database

Indexes are created to speedup restart/reload process, this index is always append too,
except in sequential-mode (see below for more information).

We use it as backend for many of our blockchain work, 0-db is not a redis replacement and never will.

# GitHub Package helper

In order to use GitHub Package Docker image, here are some environment variable you can set:
- `DATADIR`: directory of data files
- `INDEXDIR`: directory of index files
- `DATASIZE`: maximum datafiles size
- `ADMIN`: admin password
- `PROTECT`: set to 1 to enable `--protect` which requires admin password to write on default namespace

# Quick links
1. [Build targets](#build-targets)
2. [Build instructions](#build-instructions)
3. [Running](#running)
4. [Always append](#always-append)
5. [Running modes](#running-modes)
6. [Implementation](#implementation)
7. [Supported commands](#supported-commands)
8. [Namespaces](#namespaces)
9. [Hook system](#hook-system)
10. [Data offload](#data-offload)
11. [Limitation](#limitation)
12. [Tests](#tests)

# Build targets
Currently supported system:
* Linux (using `epoll`), kernel 3.17+, glibc 2.25+
* MacOS and FreeBSD (using `kqueue`)

Currently supported hardware:
* Any little-endian CPU

Currently optimized hardware:
* `x86_64` with `SSE4.2`
* `ARMv8` with `CRC32` flags (which include `Apple M1`)

This project doesn't support big-endian system for now.

# Build instructions
To build the project (library, server, tools):
* Type `make` on the root directory
* The binaries will be placed on `bin/` directory

You can build each parts separatly by running `make` in each separated directories.

> By default, the code is compiled in debug mode, in order to use it in production, please use `make release`

# Running

0-db is made to be run in network server mode (using zdbd), documentation here is about the server.
More documentation will comes about the library itself. The library lacks of documentation.

Without argument, datafiles and indexfiles will be stored on the current working directory, inside
`zdb-data` and `zdb-index` directories.

It's recommended to store index on SSD, data can be stored on HDD, fragmentation will be avoided as much
as possible (only on Linux). Please avoid CoW (Copy-on-Write) for both index and data.

## Default port

0-db listens by default on port `9900` but this can be overidden on the commandline using `--port` option.

# Always append
Data file (files which contains everything, included payload) are **in any cases** always append:
any change will result in something appened to files. Data files are immuables. If any suppression is
made, a new entry is added, with a special flag. This have multiple advantages:
- Very efficient in HDD (no seek when writing batch of data)
- More efficient for SSD, longer life, since overwrite doesn't occures
- Easy for backup or transfert: incremental copy work out-of-box
- Easy for manipulation: data is flat
- History support out-of-box, since all data are kept

Of course, when we have advantages, some cons comes with them:
- Any overwrite won't clean previous data
- Deleting data won't actually delete anything in realtime
- You need some maintenance to keep your database not exploding

Hopefuly, theses cons have their solution:
- Since data are always append, you can at any time start another process reading that database
and rewrite data somewhere else, with optimization (removed non-needed files). This is what we call
`compaction`, and some tools are here to do so.
- As soon as you have your new files compacted, you can hot-reload the database and profit, without
loosing your clients (small freeze-time will occures, when reloading the index).

Data files are never reach directly, you need to always hit the index first.

Index files are always append, except when deleting or overwriting a key. Impact are really small
only a flag is edited, and new entries are always append anyway, but the database supports to walk
over the keys, any update needs to invalidate the previous entry, in order to keep the chain in a
good health. The index is there mostly to have flexibility.

Otherwise, index works like data files, with more or less the same data (except payload) and
have the advantage to be small and load fast (can be fully populated in memory for processing).

# Running modes
On runtime, you can choose between multiple mode:
* `user`: user-key mode
* `seq`: sequential mode

**Warning**: in any case, please ensure data and index directories used by 0-db are empty, or
contains only valid database namespaces directories.

If you run `zdbd` without `--mode` argument, server will runs in `mixed mode` and allows some
`user` and `sequential` namespace on the same instance. Please see `NSSET` command to get more
information on how to choose runtime mode.

## User Key
This is a default mode, a simple key-value store.
User can `SET` their own keys, like any key-value store.
All the keys are kept in memory.

Even in this mode, the key itself is returned by the `SET` command.

## Sequential
In this mode, the key is a sequential key autoincremented.

You need to provide a null-length key, to generate a new key.
If you provide a key, this key should exists (a valid generated key), and the `SET` will update that key.

Providing any other key will fails.

The id is a little-endian integer key. Keys are not kept in memory, based on the key-id, location
on disk can be known. Running a `zdbd` in sequential-mode-only have a **really** low memory footprint.

# Implementation
This project doesn't rely on any dependencies, it's from scratch.

A rudimental and very simplified RESP protocol is supported, allowing only some commands. See below.

Each index files contains a 27 bytes headers containing a magic 4 bytes identifier,
a version, creation and last opened date and it's own file-sequential-id. In addition it contains
the mode used when it was created (to avoid mixing mode on different run).

For each entries on the index, on disk, an entry of 30 bytes + the id will be written.
In memory, 42 bytes (34 bytes + pointer for linked list) plus the key itself (limited to 256 bytes)
will be consumed.

The data (value) files contains a 26 bytes headers, mostly the same as the index one
and each entries consumes 18 bytes (1 byte for key length, 4 bytes for payload length, 4 bytes crc,
4 bytes for previous offset, 1 byte for flags, 4 byte for timestamp) plus the key and payload.

> We keep track of the key on the data file in order to be able to rebuild an index based only on datafile if needed.

Each time a key is inserted, an entry is added to the data file, then on the index.
Whenever the key already exists, it's appended to disk and entry in memory is replaced by the new one.

Each time the server starts, it loads (basicly replay) the index in memory. The index is kept in memory 
**all the time** and only this in-memory index is reached to fetch a key, index files are
never read again except during startup, reload or slow query (slow queries mean, doing some SCAN/RSCAN/HISTORY requests).

In sequential-mode, key is the location on the index, no memory usage is needed, but lot of disk access are needed.

When a key-delete is requested, the key is kept in memory and is flagged as deleted. A new entry is added
to the index file, with the according flags. When the server restart, the latest state of the entry is used.
In direct mode, the flag is overwritten in place on the index.

## Index
The current index in memory is a really simple implementation (to be improved).

It uses a rudimental kind-of hashtable. A list of branchs (2^24) is pre-allocated.
Based on the crc32 of the key, we keep 24 bits and uses this as index in the branches.

Branches are allocated only when used. Using 2^24 bits will creates 16 million index entries
(128 MB on 64 bits system).
Each branch (when allocated) points to a linked-list of keys (collisions).

When the branch is found based on the key, the list is read sequentialy.

## Read-only
You can run 0-db using a read-only filesystem (both for keys or data), which will prevent
any write and let the 0-db serving existing data. This can, in the meantime, allows 0-db
to works on disks which contains failure and would be remounted in read-only by the kernel.

This mode is not possible if you don't have any data/index already available.

# Supported commands
- `PING`
- `SET <key> <value> [timestamp]`
- `GET <key>`
- `MGET <key> [key ...]`
- `DEL <key>`
- `STOP` (used only for debugging, to check memory leaks)
- `EXISTS <key>`
- `CHECK <key>`
- `KEYCUR <key>`
- `INFO`
- `NSNEW <namespace>`
- `NSDEL <namespace>`
- `NSINFO <namespace>`
- `NSLIST`
- `NSSET <namespace> <property> <value>`
- `NSJUMP`
- `SELECT <namespace> [SECURE password]`
- `DBSIZE`
- `TIME`
- `AUTH [password]`
- `AUTH SECURE [password]`
- `SCAN [cursor]`
- `RSCAN [cursor]`
- `WAIT command | * [timeout-ms]`
- `HISTORY <key> [binary-data]`
- `FLUSH`
- `HOOKS`
- `INDEX DIRTY [RESET]`
- `DATA RAW <fileid> <offset>`
- `LENGTH <key>`
- `KEYTIME <key>`

`SET`, `GET` and `DEL`, `SCAN` and `RSCAN` supports binary keys.

Arguments `<flags>` are mandatory, arguments `[flags]` are optionals.

> Compared to real redis protocol, during a `SET`, the key is returned as response.

## SET
This is the basic `SET key value` command, key can be binary.

This command returns the key if SET was done properly or `(nil)` if you
try to update a key without modification (avoid inserting already existing data).

**Note:** admin user can specify an extra argument, timestamp, which will set the timestamp of the key
to the specified timestamp and not the current timestamp. This is needed when doing replication.

## GET (with MGET)
Retreive data, key can be binary. Returns (nil) when key doesn't exists (not found, deleted).

`GET` can only handle one key. There is `MGET` which supports multiple keys. Response is an array.

There is a hard-limit of 1023 keys at a time.

## EXISTS
Returns 1 or 0 if the key exists

## CHECK
Check internally if the data is corrupted or not. A CRC check is done internally.
Returns 1 if integrity is validated, 0 otherwise.

## KEYCUR
Returns a cursor from a key name. This cursor **should** be valid for life-time.
You can provide this cursor to SCAN family command in order to start walking from a specific
key. Even if that key was updated or deleted, the cursor contains enough data to know from
where to start looking and you won't miss any new stuff.

This cursor is a binary key.

## SCAN
Walk forward over a dataset (namespace).

- If `SCAN` is called without argument, it starts from first key (first in time) available in the dataset.
- If `SCAN` is called with an argument, it starts from provided key cursor.

If the dataset is empty, or you reach the end of the chain, `-No more data` is returned.

If you provide an invalid cursor as argument, `-Invalid key format` is returned.

Otherwise, an array (a little bit like original redis `SCAN`) is returned.
The first item of the array is the next id (cursor) you need to set to SCAN in order to continue walking.

The second element of the array is another array which contains one or more entries (keys). Each entries
contains 3 fields: the key, the size of the payload and the creation timestamp.

**Note:** the amount of keys returned is not predictable, it returns as much as possible keys
in a certain limited amount of time, to not block others clients.

Example:
```
> SCAN
1) "\x87\x00\x00\x00\x10\x00\x00\xcd4\x00\x00\x87E{\x88  # next key id to send as SCAN argument to go ahead
2) 1) 1) "\x01\x02\x03"
      2) (integer) 16                 # size of payload in byte
      3) (integer) 1535361488         # unix timestamp of creation time
   2) 1) "\xa4\x87\xd4}\xbe\x84\x1a\xba"
      2) (integer) 6                  # size of payload in byte
      3) (integer) 1535361485         # unix timestamp of creation time
```

By calling `SCAN` with each time the key responded on the previous call, you can walk forward a complete
dataset.

There is a special alias `SCANX` command which does exacly the same, but with another name.
Some redis client library (like python) expect integer response and not binary response. Using `SCANX` avoid
this issue.

In order to start scanning from a specific key, you need to get a cursor from that key first,
see `KEYCUR` command

## RSCAN
Same as scan, but backward (last-to-first key)

## NSNEW
Create a new namespace. Only admin can do this.

By default, a namespace is not password protected, is public and not size limited.

## NSDEL
Delete a namespace. Only admin can do this.

Warning:
- You can't remove the namespace you're currently using.
- Any other clients using this namespace will be moved to a special state, awaiting to be disconnected.

## NSINFO
Returns basic informations about a namespace

```
# namespace
name: default          # namespace name
entries: 0             # amount of entries
public: yes            # public writable (yes/no)
password: no           # password protected (yes/no)
data_size_bytes: 0     # total data payload in bytes
data_size_mb: 0.00     # total data payload in MB
data_limits_bytes: 0   # namespace size limit (0 for unlimited)
index_size_bytes: 0    # index size in bytes (thanks captain obvious)
index_size_kb: 0.00    # index size in KB
mode: userkey          # running mode (userkey/sequential)

## new fields
worm: no               # write-once-read-multiple mode enabled
locked: no             # lock (read-only or even write disabled) mode

next_internal_id: 0x00000000    # internal next key id
stats_index_io_errors: 0        # amount of index read/write io error
stats_index_io_error_last: 0    # last timestamp of index io error
stats_index_faults: 0           # always 0 for now
stats_data_io_errors: 0         # amount of data read/write io error
stats_data_io_error_last: 0     # timestamp of last io error
stats_data_faults: 0            # always 0 for now

index_disk_freespace_bytes: 57676599296    # free space on index partition (bytes)
index_disk_freespace_mb: 55004.69          # free space on index partition (megabytes)
data_disk_freespace_bytes: 57676599296     # free space on data partition (bytes)
data_disk_freespace_mb: 55004.69           # free space on data partition (metabytes)

data_path: /tmp/zdb-data/default           # namespace physical data path (only available for admin)
index_path: /tmp/zdb-index/default         # namespace physical index path (only available for admin)
```

Fields `stats_index_` and `stats_data_` fields are useful to know if partition on which data and index
live had issues during running time.

## NSLIST
Returns an array of all available namespaces.

## NSSET
Change a namespace setting/property. Only admin can do this.

Properties:
* `maxsize`: set the maximum size in bytes, of the namespace's data set
* `password`: lock the namespace by a password, use `*` password to clear it
* `public`: change the public flag, a public namespace can be read-only if a password is set (0 or 1)
* `worm`: « write only read multiple » flag which disable overwrite and deletion (0 or 1)
* `mode`: change index mode (`user` or `seq`)
* `lock`: set namespace in read-only or normal mode (0 or 1)
* `freeze`: set namespace in read-write protected or normal mode (0 or 1)

About mode selection: it's now possible to mix modes (user and sequential) on the same 0-db instance.
This is only possible if you don't provide any `--mode` argument on runtime, otherwise 0-db will be available
only on this mode.

It's only possible to change mode on a fully empty dataset (no deleted keys, nothing.), aka on a newly created
namespace. You can change `default` namespace aswell if it's empty.

As soon as there are a single object in the namespace, you won't be able to change mode.

`LOCK` mode won't change anything for read queries, but any update (set, del, ...) will be
denied with an error message (eg: `Namespace is temporarily locked`).

`FREEZE` mode will deny any operation on the specific namespace, read, write, update, delete operations
will be denied with an error message (eg: `Namespace is temporarily frozen`)

## NSJUMP
Force closing current index and data files and open the next id.

## SELECT
Change your current namespace. If the requested namespace is password-protected, you need
to add the password as extra parameter. If the namespace is `public` but password protected,
and you don't provide any password, the namespace will be accessible in read-only.

You can use SECURE password, like authentication (see below). A challenge is required first
(using `AUTH SECURE CHALLENGE` command). See `AUTH` below for more information.

```
>> AUTH SECURE CHALLENGE
749e5be04ca0471e
>> SELECT hello SECURE 632ef9246e9f01a3453aec8f133d1f652cccebbb
OK
```

## AUTH
If an administrator password is set, use `AUTH` command to authentificate yourself as `ADMIN`.
There is two way possible to request authentication.

### Legacy plain-text authentication
You can authenticate yourself using the simple `AUTH <password>` way. This is still supported and valid.
In the futur this will be probably disabled for security reason.

There is no encryption between client and 0-db server, any network monitor could leak
administration password.

### Secure authentication
There is a more advanced two-step authentication made to avoid plain-text password leak and safe
against replay-attack.

You can authenticate yourself using the `AUTH SECURE` command, in two step.
- First you request a challenge using `AUTH SECURE CHALLENGE`
- Then you authenticate yourself using `AUTH SECURE sha1(challenge:password)`

The challenge is session-specific random nonce. It can be used only 1 time.
Let assume the password is `helloworld`, the authentication workflow is:
```
>> AUTH SECURE CHALLENGE
708f109fbef4d656
>> AUTH SECURE 5af26c9c8bf4db0b342c42fc47e3bdae58da4578
OK
```

The secure password is constructed via `sha1(708f109fbef4d656:helloworld)`

This is the prefered method to use.

## WAIT
Blocking wait on command execution by someone else. This allows you to wait somebody else
doing some commands. This can be useful to avoid polling the server if you want to do periodic
queries (like waiting for a `SET`).

Wait takes one or two arguments: a command name to wait for and an optional timeout.
The event will only be triggered for clients on the same namespace as you (same `SELECT`),
the special command '`*`' can be used to wait on any commands.

The optional timeout argument is in milliseconds, by default, timeout is set to 5 seconds if no
timeout is provided. Timeout range can be set from 100ms to 30 minutes. Timeout precision is not
always exact and can be slightly different than expected, depending on server load.

This is the only blocking function right now. In server side, your connection is set `pending` and
you won't receive anything until someone executed the expected command or timeout occures.

When the command is triggered by someone else, you receive `+COMMAND_NAME` as response. If your reached
the timeout, you receive `-Timeout` error.

## HISTORY
This command allows you to go back in time, when your overwrite a key.

You always need to set the expected key as first argument: `HISTORY mykey`

Without more argument, you'll get information about the current state of the key.
The returned value are always the same format, an array made like this:

1. A binary string which can be used to go deeper on the history
2. The timestamp (unix) when the key was created
3. The payload of the data at that time

To rollback in time, you can follow the history by calling again the same command, with
as extra argument the first key received (a binary string). Eg: `HISTORY mykey "\x00\x00\x1b\x00\x00\x00"`

When requesting an extra argument, you'll get the previous entry. And so on...

## FLUSH
Truncate a namespace contents. This is a really destructive command, everything is deleted and no
recovery is possible (history, etc. are deleted).

This is only allowed on private and password protected namespace. You need to select the namespace
before running the command.

## HOOKS

This command is reserved to admin. It lists running (or recently running) hooks and their status.

This list contains a list of hooks. Each entry contains 6 fields:
```
> HOOKS
1) 1) "ready"
   2) (empty array)
   3) (integer) 18621
   4) (integer) 1615511907
   5) (integer) 1615511907
   6) (integer) 0
```

Values are:
1. Hook Type (name)
2. Extra arguments (depend of the type)
3. Process ID (pid)
4. Timestamp when hook were created
5. Timestamp when hook finished
6. Status Code (return code)

A still running hook will have a positive created timestamp but zero
finished timestamp.

Example of a hook triggered when 'namespace2' namespace were created:
```
> HOOKS
1) 1) "namespace-created"
   2) 1) "namespace2"
   3) (integer) 18447
   4) (integer) 1615511807
   5) (integer) 1615511818
   6) (integer) 2
```

The database keep a volatile list, only recent hooks are kept and list
is cleaned up after some time for now it's 60 seconds. This could changes.

If no recent hooks were executed, list is empty:
```
> HOOKS
(empty array)
```

## INDEX

This command have only a small subset of commands. This covers some information about index.

### INDEX DIRTY
List the current namespace index files id which were modified since last reset

### INDEX DIRTY RESET
Reset the dirty list

## DATA

This command have small internal operation on raw data file.

### DATA RAW

An internal call allows to retreive a raw data object from database only based on fileid and offset.
This method of access should only be made by a valid fileid and offset, some protection are in
place to avoid issues on wrong offset but not fully tested yet.
    
This command (only for admin) returns an array with the object requested:
  1. Key
  2. Previous Offset
  3. Integrity CRC32
  4. Flags
  5. Timestamp
  6. Payload

In addition with NSINFO, this command can be used to query a database based on fielid/offset
from another database used eg, for replication.

You can use fields `data_current_id` and `data_current_offset` from `NSINFO` to query valid offsets.

## LENGTH

Returns payload size of a key or `(nil)` if not found.

```
> SET hello world
"hello"

> LENGTH hello
(integer) 5

> LENGTH notfound
(nil)
```

## KEYTIME

Return last-update timestamp of a key or `(nil)` if not found.

```
> SET hello world
"hello"

> KEYTIME hello
(integer) 1664996517

> KEYTIME notfound
(nil)
```

# Namespaces
A namespace is a dedicated directory on index and data root directory.
A namespace is a complete set of key/data. Each namespace can be optionally protected by a password
and size limited.

You are always attached to a namespace, by default, it's namespace `default`.

## Protected mode
If you start the server using `--protect` flag, your `default` namespace will be set in read-only
by default, and protected by the **Admin Password**.

If you're running protected mode, in order to do changes on default namespace, you need to explicitly
`SELECT default [password]` to switch into read-write mode.

# Hook System
You can request 0-db to call an external program/script, as hook-system. This allows the host
machine running 0-db to adapt itself when something happen.

To use the hook system, just set `--hook /path/to/executable` on argument.
The file must be executable, no shell are invoked. Executing a shell script with shebang is valid.

When 0-db starts, it create it's own pseudo `identifier` based on listening address/port/socket.
This id is used on hooks arguments.

First argument is `Hook name`, second argument is `Generated ID`, next arguments depends of the hook.

Current supported hooks:

| Hook Name             | Action                  | Blk | Arguments                  |
| --------------------- | ----------------------- | --- | -------------------------- |
| `ready`               | Server is ready         |  *  | (none)                     |
| `close`               | Server closing (nicely) |  *  | (none)                     |
| `jump-index`          | Index incremented       |     | See below                  |
| `jump-data`           | Data incremented        |     | See below                  |
| `crash`               | Server crashed          |     | (none)                     |
| `namespaces-init`     | Pre-loading namespaces  |  *  | Index and data path        |
| `namespace-created`   | New namespace created   |     | Namespace name             |
| `namespace-updated`   | Namespace config update |     | Namespace name             |
| `namespace-deleted`   | Namespace removed       |     | Namespace name             |
| `namespace-reloaded`  | Namespace reloaded      |     | Namespace name             |
| `namespace-closing`   | Unloading namespace     |     | Namespace name             |
| `missing-data`        | Data file not found     |  *  | Missing filename           |

**WARNING**: as soon as hook system is enabled, `ready` event needs to be handled correctly.
If hook returns something else than `0`, database initialization will be stopped.

In order to get `zdbd` starting with hook, `ready` _needs_ to returns `0` to valdate server that everything
is okay and database can operate.

Namespace configuration is any metadata which can be set via `NSSET` command. Any metadata change
imply an update of `zdb-namespace` file.

Hook flagged `Blk` are blocking (waiting for hook to finish).

Special notes about:
 - `jump-index`: arguments contains old index file and new index file, in addition, last argument
    will contains a list (space separated) of id of dirty indexes. See INDEX DIRTY command above.
 - `jump-data`: arguments contains old data and new data file.
 - `namespaces-init`: blocking hook used to prepare or restore anything the database needs before
    starting namespaces initialization. This hook can be used to restore state or create empty
    namespaces. Arguments are:
     - Index path
     - Data path
 - `namespace-closing`: called when namespace is unloaded, basically this is only called on server
    stop (gracefully or crash). This hook can be used to save namespace state.
    Each namespace will trigger that hook with as arguments:
     - Namespace name
     - Current index filename
     - Current data filename
     - Dirty index list (see `jump-index` hook)

# Data offload
One latest feature of 0-db is `data offloading`. When a datafile is full (reaches `--datasize`),
the file becomes immuable (thanks always-append).

You can offload theses datafiles to make some space locally and still get a fully working database
if you're able to restore that file on demand.

The database only needs the index to fully operate, except when payload is requested (eg: with a `GET`).
This means even if datafiles are not present, you still can list, scan, delete and even update a key.
If you need the payload, the datafile needs to be restored.

**WARNING**: index cannot be offloaded and needs to be always present and available, index is not immuable !

## How to use offloader
In order to make offloader working correctly, you need to use the hook system.

Each time a datafile is full and a new one is created, hook `jump-data` (and `jump-index`) are triggered.
On theses hooks, you can copy the datafile somewhere else (eg: a slow but safe backup solution). This file will
never be updated anymore.

You can remove any datafile **except the current active one**. You only can offload (remove)
an immuable datafile. When that datafile will be needed, if it's not found, a hook will be triggered
(`missing-data`) with the filename as argument. Note that during that time, 0-db will wait for the hook,
blocking everything else.

## Dirty index
If datafiles are immuable, indexfiles is another story. Previous files can be updated, this is needed
to keep history working and other things. In order to keep the index sane in your backup solution, there
is a new way in place to keep track of what was changed, to avoid saving the full index each time.

You can issue `INDEX DIRTY` command to zdb to get a list of index id which were updated since the last reset.
The reset is made via `INDEX DIRTY RESET` command.

On the `missing-data` hook, there is an extra argument which provide the dirty index list automatically and reset
the list after. You can use that list to copy index files updated in the same time.

# Limitation
By default, datafiles are split when bigger than 256 MB.

The datafile id is stored on 32 bits, which makes maximum of 4,294,967,296 files.
Each datafile cannot be larger than 4 GB.

Each namespaces have their own datafiles, one namespace can contains maximum 16 EB (16,384 PB).

Default values (256 MB datafiles) set the limits to 1024 PB.

This limitation can be changed on startup via command line option `--datasize`, and provide (in bytes)
the size limit of a datafile. Setting `536870912` for example (which is 512 MB).

Please use always the same datasize accross multiple run, but using different size **should not** interfer.

# Tests
You can run a sets of test on a running 0-db instance.
Theses tests (optional) requires `hiredis` library.

To build the tests, type `make` in the `tests` directory.

To run the tests, run `./zdbtests` in the `tests` directory.

Warning: for now, only a local 0-db using `/tmp/zdb.sock` unix socket is supported.

Warning 2: please use an empty database, otherwise tests may fails as false-positive issue.

# Repository Owner
- [Maxime Daniel](https://github.com/maxux), Telegram: [@maxux](http://t.me/maxux)

