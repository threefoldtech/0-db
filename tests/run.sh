#!/bin/bash
set -ex

if [ ! -d src ]; then
    echo "Please run this script from root project directory"
    exit 1
fi

rm -rf /tmp/zdbtest

./src/zdb --help || true
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

./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ --admin protect
./tests/zdbtests
sleep 1

rm -rf /tmp/zdbtest

./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ --admin root
./tests/zdbtests
