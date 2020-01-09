#!/bin/bash

set -e
printf "Running Black\n"
black .

printf "Running lint ... "
flake8 datafeeds launch.py
printf " ok\n"

# FIXME: Get incremental typechecking working.
#printf "Running mypy ..."
#mypy --ignore-missing-imports datafeeds
#printf " ok\n"

printf "Running tests ...\n"
python -m unittest -v
printf "\n\nDone! You're ready to commit!\n\n"
