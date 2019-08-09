#!/bin/bash
set -ex

apt-get update
apt-get install -y build-essential git

pushd /0-db
pushd libzdb
make
popd
pushd zdbd
make STATIC=1
popd
make
popd

mkdir -p /tmp/archives/
tar -czf "/tmp/archives/0-db.tar.gz" -C /0-db/ bin
