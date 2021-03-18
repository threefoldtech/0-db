#!/bin/bash
set -ex

if [ ! -d zdbd ]; then
    echo "Please run this script from root project directory"
    exit 1
fi

rm -rf /tmp/zdbtest

# test arguments
./zdbd/zdb --help || true
./zdbd/zdb --blabla || true
./zdbd/zdb --datasize $((8 * 1024 * 1024 * 1024)) || true

# test load with slashes
./zdbd/zdb -v --dump --data /tmp/zdbtest --index /tmp/zdbtest
./zdbd/zdb -v --dump --data /tmp/zdbtest/ --index /tmp/zdbtest/

# first real test suite
./zdbd/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ --hook /bin/true --datasize $((128 * 1024 * 1024))
./tests/zdbtests
sleep 1

# reopen existing data
./zdbd/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ --dump

# simulate a segmentation fault
./zdbd/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ --hook /bin/true
pkill -SEGV zdb
sleep 1

# simulate a SIGINT (ctrl+c)
./zdbd/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ --hook /bin/true
pkill -INT zdb
sleep 1

# cleaning stuff
rm -rf /tmp/zdbtest

# starting test suite with small datasize, generating lot of file jump
./zdbd/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ --hook /bin/true --datasize 32
./tests/zdbtests
sleep 1

# cleaning stuff again
rm -rf /tmp/zdbtest

# starting with authentification
./zdbd/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/ \
    --admin protect \
    --synctime 10 \
    --mode user

./tests/zdbtests
sleep 1

rm -rf /tmp/zdbtest

# launch tcp testsuite
# by the way, separate data and index directories
./zdbd/zdb --background -v --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --admin root \
  --logfile /tmp/zdb.logs \
  --listen 127.0.0.1 --port 9900 \
  --sync

./tests/zdbtests

# open in sequential (will fails because created in another mode)
./zdbd/zdb -v --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --dump --mode seq || true

# truncate index
echo nopenopenope > /tmp/zdbtest-index/default/zdb-index-00000
./zdbd/zdb -v --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --dump || true

# clean
rm -rf /tmp/zdbtest-data
rm -rf /tmp/zdbtest-index
rm -rf /tmp/zdbtest

# test protected mode
./zdbd/zdb --data /tmp/zdbtest --index /tmp/zdbtest --dump --protect || true
./zdbd/zdb --data /tmp/zdbtest --index /tmp/zdbtest --dump --protect --admin helloworld
./zdbd/zdb --data /tmp/zdbtest --index /tmp/zdbtest --dump --maxsize 131072
rm -rf /tmp/zdbtest

# create empty dataset in direct mode
./zdbd/zdb --data /tmp/zdbtest --index /tmp/zdbtest --dump --mode direct
rm -rf /tmp/zdbtest

# create empty dataset in sequential mode
./zdbd/zdb --data /tmp/zdbtest --index /tmp/zdbtest --dump --mode seq
rm -rf /tmp/zdbtest

# trying non existing mode
./zdbd/zdb --data /tmp/zdbtest --index /tmp/zdbtest --dump --mode nonexist || true
rm -rf /tmp/zdbtest

# run tests in sequential mode
./zdbd/zdb --background --socket /tmp/zdb.sock --data /tmp/zdbtest --index /tmp/zdbtest --mode seq
./tests/zdbtests

# reload sequential database
./zdbd/zdb --socket /tmp/zdb.sock --data /tmp/zdbtest --index /tmp/zdbtest --mode seq --dump

echo "All tests done."
