# Testing in an AWS Environment

This document covers how to manually run scraper jobs in an AWS environment. Typical use cases for these instructions are:
- Debugging integration problems with a production resource. (e.g. Why can't I ship data to elasticsearch properly?)
- Debugging configuration problems with AWS Batch itself.
(e.g. Did batch use the right container? Did I put the right hostname in that environment variable?)

Debugging integration problems without some simple automation is tedious. This document explains some (very simple)
scripts that can make the process more efficient.

## Where to go...

In each environment, the ops machine will have a copy of the datafeeds repo at `~/projects/datafeeds`.
The datafeeds container is about 500 MB, so it is often more efficient to manage ECR (build images, push, retag)
from the ops machine than your laptop.

## What to do...

Datafeeds ships with several scripts:
- `build.sh`: This builds the image `gridium:datafeeds/deployed` based on local project contents.
- `run.sh`: This runs `gridium:datafeeds/deployed` (locally) against environment variables in the file `run.env`.
This is a file of environment variables (configuration) that should be kept up to date with respect to the batch environment.
- `deploy.sh`: This pushes the local `gridium:datafeeds/deployed` to ECR.

Debugging integration problems is a very slow workflow if you only use the AWS console. A more efficient process is:

- Make sure `run.env` is up to date (add any new configuration you may have introduced).
- Use `build.sh` and `run.sh` until you have a successful run on the ops machine.
- Use `deploy.sh` to push your image to ECR. Copy your environment variable changes to the batch console.
- Re-run your test on batch to confirm the changes you tested on ops.

## Test your new scraper

First, add your new scraper to a test meter in admin (admin / account / meter page / scrapers / Add new).

    ./build.sh
    ./run.sh by-meter meterOid billing|interval

If readings or bills don't appear within a few minutes, stasis transactions may be backed up.
To verify, run `select oid, target, status from stasis_transaction where target=meteOid` on the
production database. If the `status` is `verifying`, you can jump the queue by posting a message
to the immediate queue. Starting from ops:

    ssh platform2.gridium.prod
    cd ~/groovy/analytics

Edit `bifrost-immediate.groovy` to set `meterOid` to your meter, and `transactionOid` to the stasis_transaction.oid from above. Then,

    morph
    groovy analytics/bifrost-immediate.groovy

This will put a message on the immediate queue, which should go through within a few minutes to unfreeze your data.


## More details on `run.sh`

This shell script runs the `launch.py` command inside of a datafeeds docker container. In this way, you can run
the docker and python components gridium has built in more or less the same way that AWS Batch will, without the
complexity of having to schedule an AWS batch job at the console.

If you would have invoked `launch.py` via
```
python3.6 launch.py by-oid 115769 2019-01-01 2019-12-31
```
the equivalent `run.sh` command is:
```
./run.sh by-oid 115769 2019-01-01 2019-12-31
```
