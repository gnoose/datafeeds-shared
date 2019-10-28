#!/bin/bash

docker run -it \
       --env-file=run.env \
       --volume=$(pwd)/workdir:/app/workdir \
       --entrypoint=python3.6 gridium/datafeeds:deployed launch.py "${@:1}"

