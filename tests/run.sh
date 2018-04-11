#!/bin/bash
set -ex

if [ ! -d src ]; then
    echo "Please run this script from root project directory"
    exit 1
fi

rm -rf /tmp/zdbtest

./src/zdb --help || true
./src/zdb --blabla || true

./src/zdb -v --dump --data /tmp/zdbtest --index /tmp/zdbtest
./src/zdb -v --dump --data /tmp/zdbtest/ --index /tmp/zdbtest/
./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/

./tests/zdbtests
sleep 1

./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ --dump

./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/
pkill -SEGV zdb
sleep 1

./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/
pkill -INT zdb
sleep 1

rm -rf /tmp/zdbtest

./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ \
    --admin protect \
    --synctime 10 \
    --mode user

./tests/zdbtests
sleep 1

rm -rf /tmp/zdbtest

./src/zdb --background -v --data /tmp/zdbtest/ --index /tmp/zdbtest/ --admin root \
  --logfile /tmp/zdb.logs \
  --listen 127.0.0.1 --port 9900 \
  --sync

./tests/zdbtests

./src/zdb -v --data /tmp/zdbtest/ --index /tmp/zdbtest/ --dump --mode seq || true
