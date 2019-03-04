# 0-db Utilities
Here comes some utilities you can use with 0-db

## Replications
Simple replication is supported by 0-db.
Theses utilities are not yet fully stable, but should works for basic setup and main use cases.
They will be updated and improved like 0-db.

Since how 0-db was thinked, the replication doesn't works like usual master-slave works. In addition, you have
multiple replication choice.

### Namespace replication (db-replicate)
One method to replicate your database is per namespace replication listener. You start a process which will
monitor changes on a namespace in the 0-db master and replay new key changes in a slave 0-db database.

This utility doesn't support deleting key it only watch and replicate `SET` key.

To use this utility, you just have to run:
```
./db-replicate --source-host/port master-host/port --remote-host/port slave-host/port --namespace namespace-to-watch
```

This is still a PoC and doesn't support password authentication, etc. for now.

### Full database replication (db-mirror)
This utility is more advanced then `db-replicate` and provide an accurate replication between two full-set 0-db (all namespaces).

All 0-db instance can accept a **kind-of slave clients** which will receive an exact copy of received commands, in order
to replay them on a slave instance, with some extra flags to know on which namespace and timestamp to opperate.

This comes with slower performance, since the master needs to send a copy of everything to all slaves, but there is no
acknowledgment. If a slave is in a slow endpoint, connection speed will slow down master.

To use the replication, you need administrator password/right:
```
./db-mirror --source host[:port[,password]] --remote host[:port[,password]]
```

### On-demand Namespace replication (db-sync)
This works like `db-replicate` except if just do a single-shot replication of two namespace, and doesn't watch for changes.
This is useful if you want to copy the namespace from one source to a destination 0-db.
