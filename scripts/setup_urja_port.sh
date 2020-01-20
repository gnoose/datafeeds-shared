#!/bin/bash

# usage: ./setup_urja_port.sh utility

cp templates/urjanet_datasource.py ../datafeeds/urjanet/datasource/$1.py
echo "created datafeeds/urjanet/datasource/$1.py"
echo "copy contents of tasks/gridium_tasks/lib/urjanet/datasource/$1.py into datafeeds/urjanet/datasource/$1.py"

cp templates/urjanet_transformer.py ../datafeeds/urjanet/transformer/$1.py
echo "created datafeeds/urjanet/transformer/$1.py"
echo "copy contents of tasks/gridium_tasks/lib/urjanet/transformer/$1.py into datafeeds/urjanet/transformer/$1.py"

cp templates/test_urjanet_transformer.py ../datafeeds/urjanet/tests/test_urjanet_$1_transformer.py
echo "created datafeeds/urjanet/tests/test_urjanet_$1_transformer.py"
mkdir ../datafeeds/urjanet/tests/data/$1
cp ../../tasks/gridium_tasks/lib/tests/urjanet/data/$1/*.json ../datafeeds/urjanet/tests/data/$1
echo "copied test data to ../datafeeds/urjanet/tests/data/$1"

