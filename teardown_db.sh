#! /bin/bash

set -e

echo "Removing all postgres files."
rm -rf docker/pg/data

echo "Removing all mysql (urjanet) files."
rm -rf docker/mysql/data

