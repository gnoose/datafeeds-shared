## Setup

Clone this repo, and check out the branch specified for this task: **port-_UtilityId_**

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

Get the name of the utility you're porting: this is `_UtilityId_` in the steps that follow.

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
  - [scraper](../datafeeds/scrapers) - fix imports and other errors; add type annotations

## Steps

### Update [launch.py](../launch.py):

1. Add import for the utility, maintaining alphabetical order

```
from datafeeds.scrapers._UtilityId_ import datafeed as _UtilityId_
```

2. Add a line to `scraper_functions` for the utility, maintaining alphabetical order:

```
"_UtilityId_": _UtilityId_,
```

### Add CLI hook

1. Open [cli_hooks.py](../datafeeds/urjanet/scripts/cli_hooks.py)
2. Add a new class for your utility by copying and modifying an existing class in the file.

### Update scraper

1. Open the scraper class: [datafeeds/scrapers/_UtilityId_](../datafeeds/scrapers)
2. Fix imports and other errors
3. Add type annotations for function parameters.

### Run tests and push

1. Run [precommit.sh](../precommit.sh) to verify that linting and tests pass.
2. Once the tests are passing, check in and push your changes.
