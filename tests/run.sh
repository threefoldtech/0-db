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
./zdbd/zdb --datasize 32 || true

# test load with slashes
./zdbd/zdb --verbose --dump --data /tmp/zdbtest-data --index /tmp/zdbtest-index
./zdbd/zdb --verbose --dump --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/

# first real test suite
./zdbd/zdb --background --verbose --socket /tmp/zdb.sock --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --hook /bin/true --datasize $((128 * 1024 * 1024))
./tests/zdbtests
sleep 1

# reopen existing data
./zdbd/zdb --background --verbose --socket /tmp/zdb.sock --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --dump

# simulate a segmentation fault
./zdbd/zdb --background --verbose --socket /tmp/zdb.sock --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --hook /bin/true
pkill -SEGV zdb
sleep 1

# simulate a SIGINT (ctrl+c)
./zdbd/zdb --background --verbose --socket /tmp/zdb.sock --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --hook /bin/true
pkill -INT zdb
sleep 1

# cleaning stuff
rm -rf /tmp/zdbtest

# starting test suite with small datasize, generating lot of file jump
./zdbd/zdb --background --verbose --socket /tmp/zdb.sock --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --hook /bin/true --datasize $((518 * 1024))
./tests/zdbtests
sleep 1

# cleaning stuff again
rm -rf /tmp/zdbtest-data /tmp/zdbtest-index

# starting with authentification
./zdbd/zdb --background --verbose --socket /tmp/zdb.sock --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ \
    --admin protect \
    --synctime 10 \
    --mode user

./tests/zdbtests
sleep 1

rm -rf /tmp/zdbtest-data /tmp/zdbtest-index

# launch tcp testsuite
./zdbd/zdb --background --verbose --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --admin root \
  --logfile /tmp/zdb.logs \
  --listen 127.0.0.1 --port 9900 \
  --sync

./tests/zdbtests

# open in sequential (will fails because created in another mode)
./zdbd/zdb --verbose --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --dump --mode seq || true

# truncate index
echo nopenopenope > /tmp/zdbtest-index/default/zdb-index-00000
./zdbd/zdb --verbose --data /tmp/zdbtest-data/ --index /tmp/zdbtest-index/ --dump || true

# clean
rm -rf /tmp/zdbtest-data /tmp/zdbtest-index

# test protected mode
./zdbd/zdb --data /tmp/zdbtest-data --index /tmp/zdbtest-index --dump --protect || true
./zdbd/zdb --data /tmp/zdbtest-data --index /tmp/zdbtest-index --dump --protect --admin helloworld
./zdbd/zdb --data /tmp/zdbtest-data --index /tmp/zdbtest-index --dump --maxsize 131072
rm -rf /tmp/zdbtest-data /tmp/zdbtest-index

# create empty dataset in direct mode
./zdbd/zdb --data /tmp/zdbtest-data --index /tmp/zdbtest-index --dump --mode direct
rm -rf /tmp/zdbtest-data /tmp/zdbtest-index

# create empty dataset in sequential mode
./zdbd/zdb --data /tmp/zdbtest-data --index /tmp/zdbtest-index --dump --mode seq
rm -rf /tmp/zdbtest-data /tmp/zdbtest-index

# trying non existing mode
./zdbd/zdb --data /tmp/zdbtest-data --index /tmp/zdbtest-index --dump --mode nonexist || true
rm -rf /tmp/zdbtest-data /tmp/zdbtest-index

# run tests in sequential mode
./zdbd/zdb --background --socket /tmp/zdb.sock --data /tmp/zdbtest-data --index /tmp/zdbtest-index --mode seq
./tests/zdbtests

# reload sequential database
./zdbd/zdb --socket /tmp/zdb.sock --data /tmp/zdbtest-data --index /tmp/zdbtest-index --mode seq --dump

echo "All tests done."
