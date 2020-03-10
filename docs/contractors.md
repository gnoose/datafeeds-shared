# Working with contractors

Contractors should be added as outside collaborators to the
[datafeeds-shared](https://github.com/Gridium/datafeeds-shared) repo, not this one.
This prevents access to the CircleCI configuration that we don't want to share.

## Setup for Gridium employees

Clone the `datafeeds-shared` repo

    git clone https://github.com/Gridium/datafeeds-shared.git

Add `datafeeds` as a remote:

    cd datafeeds-shared
    git remote add upstream https://github.com/gridium/datafeeds

Before starting a new job, sync `datafeeds` changes to `datafeeds-shared`. From `datafeeds-shared`:

    git checkout master
    git pull --rebase # get latest datafeeds-shared
    git fetch upstream  # get latest datafeeds
    git merge upstream/master # merge datafeeds master into datafeeds-shared master
    git push origin master # push to datafeeds-shared master

Create a pull request in `datafeeds-shared` for the contractor;
copy the contents of [porting_urjanet.md](porting_urjanet.md) into the PR description.

Assign the job to a contractor. They should push their changes to a branch in the `datafeeds-shared` repo.

When the PR is ready, merge it on GitHub as usual. Then sync `datafeeds-shared` changes to `datafeeds`. From `datafeeds-shared`:

    git checkout master
    git pull --rebase
    git fetch upstream  # get latest datafeeds
    git merge upstream/master # merge datafeeds-shared work into datafeeds master
    git push upstream master # push to datafeeds master


## Setup for contractors

Contractors should work in the `datafeeds-shared` repo.

See directions in [porting_urjanet.md](porting_urjanet.md).

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
