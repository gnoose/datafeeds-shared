#!/bin/bash

# Pull a docker image from ECR based upon githash.

$(aws ecr get-login --no-include-email --region us-west-1)

docker pull 891208296108.dkr.ecr.us-west-1.amazonaws.com/datafeeds:$1
docker tag 891208296108.dkr.ecr.us-west-1.amazonaws.com/datafeeds:$1 gridium/datafeeds:dev
