#!/bin/bash

set -e
printf "Running Black\n"
black .

printf "Running lint ... "
flake8 datafeeds launch.py
printf " ok\n"

printf "Running mypy ..."
mypy --no-strict-optional datafeeds launch.py
printf " ok\n"

printf "Running tests ...\n"
python -m unittest -v
printf "\n\nDone! You're ready to commit!\n\n"

printf "Running smoke test for launch.py...\n"
if python launch.py --help > /dev/null; then
    echo "Smoke test passed."
else
    echo "Smoke test failed."
fi
