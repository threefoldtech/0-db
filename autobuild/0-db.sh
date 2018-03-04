#!/bin/bash
set -ex

apt-get update
apt-get install -y build-essential

pushd /0-db
make
popd

mkdir -p /tmp/archives/
tar -czf "/tmp/archives/0-db.tar.gz" -C /0-db/ bin
