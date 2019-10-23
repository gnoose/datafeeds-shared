#!/bin/bash

$(aws ecr get-login --no-include-email --region us-west-1)
docker tag gridium/datafeeds:deployed 891208296108.dkr.ecr.us-west-1.amazonaws.com/datafeeds:deployed
docker push 891208296108.dkr.ecr.us-west-1.amazonaws.com/datafeeds:deployed
