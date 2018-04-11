#!/bin/bash
set -ex

if [ ! -d src ]; then
    echo "Please run this script from root project directory"
    exit 1
fi

rm -rf /tmp/zdbtest

# test arguments
./src/zdb --help || true
./src/zdb --blabla || true

# test load with slashes
./src/zdb -v --dump --data /tmp/zdbtest --index /tmp/zdbtest
./src/zdb -v --dump --data /tmp/zdbtest/ --index /tmp/zdbtest/

# first real test suite
./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/
./tests/zdbtests
sleep 1

# reopen existing data
./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ --dump

# simulate a segmentation fault
./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/
pkill -SEGV zdb
sleep 1

# simulate a SIGINT (ctrl+c)
./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/
pkill -INT zdb
sleep 1

# starting with authentification
rm -rf /tmp/zdbtest

./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ \
    --admin protect \
    --synctime 10 \
    --mode user

./tests/zdbtests
sleep 1

rm -rf /tmp/zdbtest

# launch tcp testsuite
# by the way, separate data and index directories
./src/zdb --background -v --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --admin root \
  --logfile /tmp/zdb.logs \
  --listen 127.0.0.1 --port 9900 \
  --sync

./tests/zdbtests

# open in sequential (will fails because created in another mode)
./src/zdb -v --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --dump --mode seq || true

# truncate index
echo nopenopenope > /tmp/zdbtest-index/default/zdb-index-00000
./src/zdb -v --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --dump || true

# clean
rm -rf /tmp/zdbtest-data
rm -rf /tmp/zdbtest-index

# create empty dataset in direct mode
./src/zdb --data /tmp/zdbtest --index /tmp/zdbtest --dump --mode direct
rm -rf /tmp/zdbtest

./src/zdb --data /tmp/zdbtest --index /tmp/zdbtest --dump --mode nonexist || true
rm -rf /tmp/zdbtest

./src/zdb --data /tmp/zdbtest --index /tmp/zdbtest --dump --mode block
