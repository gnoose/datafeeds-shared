# Q4 2019 Hackathon

This quarter we're going to try building some new scrapers on top of AWS batch.
This document goes over (roughly) what you need to do in order to set up a new urjanet scraper.

# Goals

- Get familiar with building urjanet scrapers.
- Explore running scrapers with a different model for parallel computing.

# Background

Currently we use `celery` to distribute scraper jobs in production. The disadvantages of the current setup are:

1. We have to manage some of the systems that support celery (flower, Redis).
2. Flower loses job history after a set period (hours).
3. No mechanism for scaling up/down the number of celery workers based on workload. The system is slow.
4. Celery workers are stateful, we'd prefer to use docker to keep a fixed definition of a worker server.
5. Scraper jobs share the same workers. One scraper failure can impede its peers.

Some advantages of AWS Batch:

1. Easy to add/remove workers.
2. Batch manages compute resources and job scheduling for us, so we can focus on what happens in the worker container.
3. Since scrapers are containerized (created and destroyed on each run), scraper runs are guaranteed to be independent.

In either `tasks` or `datafeeds`, the application has two major pieces:

1. Scraper code: These are the procedures for extracting interval and bill data for a particular utility.
2. Job management code: This defines how/where the scraper code will be run, and certain metadata common
    to all scrapers.

The details of step (1) are independent of whether we use Batch or Celery to run the scraper. Therefore, it's 
reasonable to think that we could "transplant" our existing scraper code onto an AWS Batch based system and obtain:
1. A system that costs less to run
2. runs our scraper workload much faster
3. and resolves some longstanding technical debt.

# Resources

Before you begin, you'll need some resources. Make sure you have:

1. A copy of the Urjanet database (warning: 1.1GB unzipped). `scp ops:/builds/urjanet_dumps/urjanet.sql.gz .`

2. The urjanet password so that you can download bill PDFs. (Share this the day of the hackathon.)

3. A docker-compose setup. Run the Urjanet [setup steps](./urjanet_setup.md) and PostgreSQL 
[setup steps](./database_setup.md).

# Steps

## Set up a new Urjanet Scraper

(see also: https://github.com/Gridium/tasks/blob/master/gridium_tasks/lib/urjanet/README.md)

1. Set up a new branch for your work. Choose a tag (e.g. City of Watauga Water -> `watauga`).

2. Create/activate a python 3.6 environment:

```
pyenv virtualenv 3.6.4 datafeeds
pyenv activate datafeeds
pip install -r requirements.txt
source test-config

```

3. From the top level of your `datafeeds` repo, run `export PYTHONPATH=$(pwd):$PYTHONPATH`

4. Review bills for the utility you're working on
  - go to https://urjanet-explorer.gridium.com/ and login with your Gridium Google account
  - search in the page for the utility name
  - click the account name to view a list of bill records for the account
  - click the pdf link to download a view a PDF of the bill (requires Urjanet password)

5. Log into your Urjanet database and look up some bills in the `Accounts` table (AccountNumber from urjanet-explorer). You may want to write down a table of the bill period dates, cost, use, peak, and account numbers you found, for reference.

```
    select c.IntervalStart, c.IntervalEnd, c.ChargeAmount, ChargeUnitsUsed
    from Charge c, Meter m, Account a
    where a.AccountNumber='151009077' and a.PK=c.AccountFK and a.PK=m.AccountFK and m.PK=c.MeterFK;
```

6. Write a datasource class for the scraper to load data from Urjanet. See [WataugaDatasource](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/datasource/watauga.py)

7. Write a transformer for the scraper to adjust any unusual cases in the Urjanet data. See [WataugaTransformer](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/transformer/watauga.py).

8. Add your new utility to [cli_hooks.py](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/scripts/cli_hooks.py)

9. Create a data dump for your test account(s). Review and compare to PDF version of bills.

```
    cd datafeeds/urjanet/scripts
    mkdir ../tests/data/watauaga
    python dump_urja_json.py watauga 151009077 > ../tests/data/watauga/input_151009077.json
```

10. Run your transformer on the extracted data. . Review and compare to PDF version of bills.

```
    python transform_urja_json.py ../tests/data/watauga/input_151009077.json > ../tests/data/watauga/expected_151009077.json
```

11. Write a test; see [TestUrjanetWataugaTransformer](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/tests/test_urjanet_watauga_transformer.py)


## Add your new scraper to the job management tool / launch script.

1. Open the [launch script](../launch.py).

2. Using City of Watauga as an example, add a function similar to `watauga_ingest_batch` and a new key to the
 `scraper_functions` map.

## Test your scraper.

1. Use the script `create_data_sources.py` to configure a meter in your dev setup to use your new datasource. Get the account oid from the hex id with `select oid from snapmeter_account where hex_id='5661eab691f1a2508278c01d`.
In the example below, we add `watauga-urjanet` to Snapmeter Account 999, Meter 4505071289158471.

    ```python scripts/create_data_sources.py 999 4505071289158471 watauga-urjanet city-of-watauga-demo```

2. Run your scraper via the launch script: `python launch.py by-oid 1907085797026 2019-01-01 2019-12-31`. This runs your scraper exactly the same way AWS batch will. If everything
works, you should see something like this:
    ```
    /Users/jsthomas/.pyenv/versions/datafeeds/bin/python /Users/jsthomas/repos/datafeeds/launch.py by-oid 4 2019-08-01 2019-12-01
    2019-10-02 17:49:07,973 : INFO : Scraper Launch Settings:
    2019-10-02 17:49:07,973 : INFO : Meter Data Source OID: 4
    2019-10-02 17:49:07,973 : INFO : Meter: Meter #3 (4505071289158471)
    2019-10-02 17:49:07,973 : INFO : Account: Tacolicious (999)
    2019-10-02 17:49:07,973 : INFO : Scraper: watauga-urjanet
    2019-10-02 17:49:07,974 : INFO : Date Range: 2019-08-01 - 2019-12-01
    2019-10-02 17:49:07,987 : INFO : Launching Urjanet Scraper: city-of-watauga
    2019-10-02 17:49:07,987 : INFO : Username:   None
    2019-10-02 17:49:07,987 : INFO : Start Date: 2019-08-01
    2019-10-02 17:49:07,987 : INFO : End Date:   2019-12-01
    2019-10-02 17:49:07,987 : INFO : Configuration:
    2019-10-02 17:49:07,987 : INFO : 	scrape_bills: True
    2019-10-02 17:49:07,987 : INFO : 	scrape_readings: False
    2019-10-02 17:49:07,987 : INFO : 	urja_datasource: <datafeeds.urjanet.datasource.watauga.WataugaDatasource object at 0x10eeaa6d8>
    2019-10-02 17:49:07,987 : INFO : 	urja_transformer: <datafeeds.urjanet.transformer.watauga.WataugaTransformer object at 0x10eebc5c0>
    2019-10-02 17:49:07,987 : INFO : 	utility_name: city-of-watauga
    2019-10-02 17:49:07,987 : INFO : 	fetch_attachments: True
    2019-10-02 17:49:08,252 : INFO : ================================================================================
    2019-10-02 17:49:08,252 : INFO : Final Billing Summary --- This is what would have been uploaded to platform.
    2019-10-02 17:49:08,252 : INFO : ================================================================================
    2019-10-02 17:49:08,253 : INFO : Start       End         Cost        Use         Peak       Has PDF   
    2019-10-02 17:49:08,253 : INFO : 2019-07-11  2019-08-14  1972.73     4810.0      None       False     
    2019-10-02 17:49:08,253 : INFO : 2019-08-15  2019-09-12  2396.16     9030.0      None       False     
    2019-10-02 17:49:08,253 : INFO : ================================================================================
    ```

## Finally, test in Dev.

The final step is to try to accomplish the same run in our dev environment, on AWS batch.

1. Commit your changes to your branch.

2. SSH to the `energy-ops` EC2 in dev.

3. Go to `projects/datafeeds` and pull your changes in.

4. Build your container and push to ECR.
    ```
    $(aws ecr get-login --no-include-email --region us-east-1)
    docker build -t gridium/datafeeds:<YOUR TAG> .
    docker tag datafeeds:latest 634855895757.dkr.ecr.us-east-1.amazonaws.com/datafeeds:<YOUR TAG>
    docker push 634855895757.dkr.ecr.us-east-1.amazonaws.com/datafeeds:<YOUR TAG>
    ```

5. Create a revised Job Definition that uses your container in the Batch console.

6. Configure a meter in the dev environment to use your data source, as you did locally.

7. Finally, try running your job on AWS Batch. If successful, you should see the same output from your local test
 in the cloudwatch logs.
