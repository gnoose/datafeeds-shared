#!/usr/bin/env python

import os

SLACK_TOKEN: str = os.environ.get("SLACK_TOKEN")
SLACK_CHANNEL: str = os.environ.get("SLACK_CHANNEL", "#slack-debug")
