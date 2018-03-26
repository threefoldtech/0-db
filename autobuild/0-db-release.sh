#!/bin/bash
set -ex

apt-get update
apt-get install -y build-essential git

pushd /0-db
pushd src
make release STATIC=1
popd
make
popd

mkdir -p /tmp/archives/
tar -czf "/tmp/archives/0-db-release.tar.gz" -C /0-db/ bin
