#!/bin/bash

set -e

source test-config

printf "Running Lint ..."
flake8 datafeeds
printf " ok\n"

# FIXME: Get incremental typechecking working.
#printf "Running mypy ..."
#mypy --ignore-missing-imports datafeeds
#printf " ok\n"

printf "Running tests ...\n"
python -m unittest -v
printf "\n\nDone! You're ready to commit!\n\n"
