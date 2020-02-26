# Porting scrapers

This document describes how to port a scraper.

## Setup

Clone this repo, and check out the branch specified for this task.

Install and activate a python 3.6 environment:

```
pyenv virtualenv 3.6.4 datafeeds
pyenv activate datafeeds
```

Install dependencies:

```
pip install -r requirements.txt
pip install -r dev-requirements.txt
```

Get the name of the utility you're porting: this is `utilityId` in the steps that follow.

## Background

We need to move and update python code from an older framework to a newer one.
The code was copied from the older framework to a branch in this repo.

The goals for each port are:

  - fix imports and other errors
  - add type annotations
  - get lint and tests to pass

The task is complete when running `./precommit.sh` finishes without errors.

These are the areas that need to be updated:

  - [launch.py](../launch.py) - add new utility import and key
  - [cli_hooks.py](../datafeeds/urjanet/scripts/cli_hooks.py) - add hook for creating test fixtures
  - [datasource](../datafeeds/urjanet/datasource) - fix imports and other errors; add type annotations
  - [transformer](../datafeeds/urjanet/transformer) - fix imports and other errors; add type annotations
  - [tests](../datafeeds/urjanet/tests/) - run the transformer against an input fixture and compare the results to an expected fixture

## Steps

### Update [launch.py](../launch.py):

1. Add import for the utility, maintaining alphabetical order

```
from datafeeds.urjanet.datasource.utilityId import datafeed as utilityId
```

2. Add a line to `scraper_functions` for the utility, maintaining alphabetical order:

```
"utilityId-urjanet": utilityId,
```

### Add CLI hook

1. Open [cli_hooks.py](../datafeeds/urjanet/scripts/cli_hooks.py)
2. Add a new class for your utility by copying and modifying an existing class in the file.

### Update datasource

1. Open the datasource class: [datafeeds/urjanet/datasource/utilityId](../datafeeds/urjanet/datasource)
2. Fix imports and other errors (ie add an import for the new transformer class)
3. Add type annotations for function parameters.

### Update transformer

1. Open the transformer class: [datafeeds/urjanet/transformer/utilityId](../datafeeds/urjanet/transformer)
2. Fix imports and other errors.
3. Add type annotations for function parameters.

### Update tests

1. Run `python transform_urja_json.py ../tests/data/utilityId/utilityId01_input.json utilityId > ../tests/data/utilityId/utilityId01_expected`
2. Update the test: [datafeeds/urjanet/tests/test_urjanet_utilityId_transformer.py] to uncomment the test and fix the input and expected filenames.

### Run tests and push

1. Run [precommit.sh](../precommit.sh) to verify that linting and tests pass.
2. Once the tests are passing, check in and push your changes.
