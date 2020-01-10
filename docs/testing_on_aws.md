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

Debugging integration problems is a very slow workflow if you only use the AWS console. A more efficient process is to run locally from ops:

- Make sure `run.env` is up to date (add any new configuration you may have introduced).
- Use `build.sh` and `run.sh` until you have a successful run on the ops machine.
- Use `deploy.sh` to push your image to ECR. (see [deploy.md](deploy.md))

Once your job is running successfully from ops, you can run it via AWS Batch the same way webapps does with [launch_datafeed.py](https://github.com/Gridium/webapps/blob/master/scripts/launch_datafeed.py) in webapps. You'll need the name of your scraper and a meter oid (the meter must already be provisioned with the scraper). From ops:

```
cd ~/projects/webapps
source venv/bin/activate
cd scripts
python launch_datafeed.py 1862307348482 bloom
```

This does the same database query that webapps uses to schedule jobs, so it should reveal any issues with product enrollment, scraper names, etc. If the meter and scraper were found, this will schedule a job in the `datafeeds-high` queue and print something like `<Datafeeds Job: uuid=d2915084-8a93-4cf7-8745-d69c75fc1409, source=117607, status=SUBMITTED, updated=2020-01-10 19:09:54.822632>`

Your job should immediately be visible on the [AWS Batch dashboard](https://us-west-1.console.aws.amazon.com/batch/home?region=us-west-1#/dashboard), in the `datafeeds-high` row. You can also go directly to the job by adding the job UUID to the URL: https://us-west-1.console.aws.amazon.com/batch/home?region=us-west-1#/jobs/queue/arn:aws:batch:us-west-1:891208296108:job-queue~2Fdatafeeds-high/job/d2915084-8a93-4cf7-8745-d69c75fc1409

Once the job starts, it should write a record to Elasticsearch which you can find in Kibana:

  - get Kibana credentials from LastPass
  - go to Kibana: https://6e4cab9dd2954f47a4a69440dc0247c0.us-east-1.aws.found.io:9243/app/kibana
  - type your meter oid into the the Filters box


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
