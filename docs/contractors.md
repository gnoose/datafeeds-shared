# Working with contractors

Contractors should be added as outside collaborators to the
[datafeeds-shared](https://github.com/Gridium/datafeeds-shared) repo, not this one.
This prevents access to the CircleCI configuration that we don't want to share.

## One time setup for Gridium employees

Clone the `datafeeds-shared` repo

    git clone https://github.com/Gridium/datafeeds-shared.git

Add `datafeeds` as a remote:

    cd datafeeds-shared
    git remote add upstream https://github.com/gridium/datafeeds

## Task setup for Gridium employees

Before starting a new job, sync `datafeeds` changes to `datafeeds-shared`. From `datafeeds-shared`:

    git checkout master
    git pull --rebase # get latest datafeeds-shared
    git fetch upstream  # get latest datafeeds
    git merge upstream/master # merge datafeeds master into datafeeds-shared master
    git push origin master # push to datafeeds-shared master

Create a test datasource for the scraper.

Get credentials from ops

    cd ~/projects/webapps:
    python scripts/credentials.py meterOid scraper-name

From energy-dev-ops:/home/ubuntu/projects/datafeeds or one of the Cloud9 environments, create a datasource in the dev database:

    cd datafeeds-shared
    python scripts/create_test_data_source.py scraper-name username password --service_id 000 --utility_account_id 000

This will print a command to run the scraper (`python launch.py by-oid 123 2021-04-01 2021-04-08`).
Make sure to include this in the PR description.

In `datafeeds-shared`, create a branch for the task. A PR requires a commit, so either add a comment
to the scrape code, or create an empty commity: `git commit --allow-empty -m "start scraper fix"`

Then create a PR.

Starter PR text:

```
## setup
From https://aws.amazon.com/, click Sign In to the Console. Use `gridium-dev` as the account ID, and sign
in with your IAM credentials. Go to the [Cloud9 home page](https://console.aws.amazon.com/cloud9/home).

Click **Open IDE** to start the **gridium-datafeeds-1** environment.

Get the environment set up:

    pyenv activate datafeeds
    cd datafeeds-shared
    scripts/start_chrome.sh
    git checkout master
    git pull
    git checkout 3888-svp-billing


## task description

Describe the issue. Add the URL to get to the site. Describe how to confirm that the issue is fixed.

Run the scraper: `python launch.py by-oid 44 2020-06-06 2020-12-07`

The code for this scraper is https://github.com/gridium/datafeeds-shared/...

Update the code to get the scraper to complete successfully. Before committing your changes, run these and fix any issues:

    black .
    flake8 datafeeds launch.py
    mypy --no-strict-optional datafeeds launch.py

Add the files, commit, and push:

    git commit
    git push

Finally, request a review on the pull request.
```

Assign the job to a contractor. They should push their changes to a branch in the `datafeeds-shared` repo.

When the PR is ready, merge it on GitHub as usual. Then sync `datafeeds-shared` changes to `datafeeds`. From `datafeeds-shared`:

    git checkout master
    git pull --rebase
    git fetch upstream  # get latest datafeeds
    git merge upstream/master # merge datafeeds-shared work into datafeeds master
    git push upstream master # push to datafeeds master


## Setup for contractors

Contractors should work in the `datafeeds-shared` repo.

See [Cloud9 docs](docs/cloud9.md).

## Testing

- create a datasource in the `gridium_test` database with [webapps/scripts](https://github.com/Gridium/webapps/tree/master/scripts/create_test_data_source.py):

    python create_test_data_source.py 123 datasource_name username password

- get values of `AES_KEY` from webapps/localconfig.py (used to encrypt username and password) and set it in environment

    export AES_KEY="webapps key here"

- get local config setup from datafeeds:

    cd ../datafeeds
    source local.env

- run scraper with test meter datasource created above:

    python launch.py by-oid 2027 2019-03-01 2020-03-01

- get production bills from ops

    cd ~/projects/webapps
    source venv/bin/activate
    cd scripts
    python export_bills.py meterOID 2019-03-01 2020-03-01

- copy ops:/tmp/meterOID.csv to local:

    scp ops:/tmp/1817245075326.csv workdir

- compare:

    diff workdir/1817245075326.csv workdir/bills.csv
