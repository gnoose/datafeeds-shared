#!/bin/bash

# Launch a dockerized scraper run from the command line.

docker run -it \
       --env-file=run.env \
       --volume=$(pwd)/workdir:/app/workdir \
       --entrypoint=python3 gridium/datafeeds:dev launch.py "${@:1}"

