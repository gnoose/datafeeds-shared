#!/bin/bash

docker run -it --env-file=run.env --entrypoint=python3.6 gridium/datafeeds:deployed launch.py "${@:1}"

