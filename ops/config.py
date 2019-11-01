#!/usr/bin/env python

import os

AWS_REGION_NAME: str = os.environ.get("AWS_REGION_NAME")
SLACK_TOKEN: str = os.environ.get("SLACK_TOKEN")
SLACK_CHANNEL: str = os.environ.get("SLACK_CHANNEL", "#slack-debug")
