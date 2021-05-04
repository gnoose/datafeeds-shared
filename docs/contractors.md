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

    cd ~/projects/webapps
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

Click **Open IDE** to start the **gridium-datafeeds-1** environment. You can set up the environment preferences however you want.

Get the environment set up:

    pyenv activate datafeeds
    cd datafeeds-shared
    scripts/start_chrome.sh
    git checkout master
    git pull
    git checkout branch-name
    git config user.name "First Last"
    git config user.email you@email.com

When you run `git pull`, you'll be prompted for a username and password. Use your GitHub username for the username, and a personal access token for password. To create one, see [Creating a personal access token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token). On the `git config` step, if you have GitHub set to hide your email, use your private email from https://github.com/settings/emails.

See also [cookbook.md](cookbook.md) for how to get credentials, see the browser, take screenshots, etc.

## task description

Describe the issue. Add the URL to get to the site. Describe how to confirm that the issue is fixed.

Run the scraper: `python launch.py by-oid 44 2020-06-06 2020-12-07`

The code for this scraper is https://github.com/gridium/datafeeds-shared/...

Update the code to get the scraper to complete successfully. 

## verification

When complete, the scraper should produce output like this (values will differ, for illustration only):

    2021-05-03 04:46:16,154 : INFO : Final Scraped Summary
    2021-05-03 04:46:16,154 : INFO : ================================================================================
    2021-05-03 04:46:16,154 : INFO : Start       End         Cost        Use         Peak       Has PDF    Utility Code
    2021-05-03 04:46:16,154 : INFO : 2019-04-09  2019-05-08  10144.61    9489.0      0.0        True       GNR1 Gas Service to Small Commercial Customers
    2021-05-03 04:46:16,154 : INFO : 2019-05-09  2019-06-07  8906.65     8566.0      0.0        True       GNR1 Gas Service to Small Commercial Customers
    2021-05-03 04:46:16,154 : INFO : 2019-06-08  2019-07-09  7875.76     7425.0      0.0        True       GNR1 Gas Service to Small Commercial Customers

    2021-04-30 05:15:10,182 : INFO : Summary of all Scraped Partial Bills
    2021-04-30 05:15:10,182 : INFO : ================================================================================
    2021-04-30 05:15:10,182 : INFO : Start       End         Cost        Use         Peak       Has PDF    Utility Code
    2021-04-30 05:15:10,182 : INFO : 2021-02-23  2021-03-23  1287.64     25434.832   0.0        True       A10SX-Bright Choice


    2021-04-28 20:48:05,429 : INFO : Final Interval Summary
    2021-04-28 20:48:05,429 : INFO : 2021-04-25: 96 intervals. 0.4 net kWh, 0 null values.
    2021-04-28 20:48:05,430 : INFO : 2021-04-26: 96 intervals. 0.4 net kWh, 0 null values.
    2021-04-28 20:48:05,430 : INFO : 2021-04-27: 96 intervals. 0.0 net kWh, 96 null values.
    2021-04-28 20:48:05,430 : INFO : 2021-04-28: 96 intervals. 0.0 net kWh, 96 null values.
    2021-04-28 20:48:05,431 : INFO : Wrote interval data to /app/workdir/readings.csv.
    2021-04-28 20:48:05,432 : INFO : all statuses: bills=None readings=Status.SUCCEEDED, pdfs=None, tnd=None, gen=None, meta=None
    20

Before committing your changes, run these and fix any issues:

    black .
    flake8 datafeeds launch.py
    mypy --no-strict-optional datafeeds launch.py

Add the files, commit, and push:

    git commit
    git push

Finally, request a review on the pull request.
```

Assign the job to a contractor. They should push their changes to a branch in the `datafeeds-shared` repo.

Open the Cloud9 IDE assigned to the PR, and make sure it is shared with the contractor's AWS username.

When the PR is ready, merge it on GitHub as usual. Then sync `datafeeds-shared` changes to `datafeeds`. From `datafeeds-shared`:

    git checkout master
    git pull --rebase
    git fetch upstream  # get latest datafeeds
    git merge upstream/master # merge datafeeds-shared work into datafeeds master
    git push upstream master # push to datafeeds master


## Setting up a new contractor

Ask the contractor for their Github username and an email address.

Add the contractor as a single-channel guest to Slack (#scraper-dev-contract).

Add the contractor as an outside collaborator to [datafeeds-shared](https://github.com/Gridium/datafeeds-shared/settings/access) with write access.

Add a new [IAM user](https://console.aws.amazon.com/iam/home?region=us-east-1#/users$new?step=details) (first initial, last name) to the `gridium-dev` AWS account with

  - Access type: AWS Management Console access
  - Console password: Autogenerated password
  - Require password reset: checked

On the Add user to group page, add user to Cloud9User group

Send the new contractor:

  - AWS username and autogenerated password
  - link to datafeeds-shared PR

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
