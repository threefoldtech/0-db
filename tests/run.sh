#!/bin/bash
if [ ! -d src ]; then
    echo "Please run this script from root project directory"
    exit 1
fi

rm -rf /tmp/zdbtest

./src/zdb --help
./src/zdb -v --dump --data /tmp/zdbtest --index /tmp/zdbtest
./src/zdb -v --dump --data /tmp/zdbtest/ --index /tmp/zdbtest/
./src/zdb --background -v --socket /tmp/zdb.sock --data /tmp/zdbtest/ --index /tmp/zdbtest/

./tests/zdbtests
