# Q4 2019 Hackathon

This quarter we're going to try building some new scrapers on top of AWS batch.
This document goes over (roughly) what you need to do in order to set up a new urjanet scraper.

# Goals

- Get familiar with building urjanet scrapers.
- Explore running scrapers with a different model for distributed computing.

# Background

Currently we use `celery` to distribute scraper jobs in production. The disadvantages of the current setup are:

1. We have to manage some of the systems that support celery (flower, Redis).
2. Flower loses job history after a set period (hours).
3. No mechanism for scaling up/down the number of celery workers based on workload. The system is slow.
4. Celery workers are stateful. We'd prefer to use docker to keep a fixed definition of a worker server.
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

Make sure you have:

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
    pip install flake8
    ```

3. From the top level of your `datafeeds` repo, run `export PYTHONPATH=$(pwd):$PYTHONPATH`

4. Review bills for the utility you're working on
    - go to https://urjanet-explorer.gridium.com/ and login with your Gridium Google account
    - search in the page for the utility name
    - click the account name to view a list of bill records for the account
    - click the pdf link to download a view a PDF of the bill (requires Urjanet password)

5. Log into your Urjanet database and look up some bills in the `Accounts` table (AccountNumber from urjanet-explorer). See https://github.com/Gridium/tasks/blob/master/gridium_tasks/lib/urjanet/README.md#model for more info on the Urjanet schema. You may want to write down a table of the bill period dates, cost, use, peak, and account numbers you found, for reference.

    ```
    select c.IntervalStart, c.IntervalEnd, c.ChargeAmount, ChargeUnitsUsed, a.UtilityProvider
    from Charge c, Meter m, Account a
    where a.AccountNumber='07292000' and a.PK=c.AccountFK and a.PK=m.AccountFK and m.PK=c.MeterFK;
    ```

6. Write a datasource class in `datafeeds/urjanet/datasource` for the scraper to load data from Urjanet. See [WataugaDatasource](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/datasource/watauga.py).

7. Write a transformer class in `datafeeds/urjanet/transformer` for the scraper to adjust any unusual cases in the Urjanet data. See [WataugaTransformer](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/transformer/watauga.py).

8. Add your new utility to [cli_hooks.py](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/scripts/cli_hooks.py)

9. Create a data dump for your test account(s). Review and compare to PDF version of bills.

    ```
    cd datafeeds/urjanet/scripts
    mkdir ../tests/data/watauaga
    python dump_urja_json.py watauga 01202000 > ../tests/data/watauga/input_01202000.json
    ```

10. Run your transformer on the extracted data. Review and compare to PDF version of bills.

    ```
    python transform_urja_json.py ../tests/data/watauga/input_01202000.json watauga > ../tests/data/watauga/expected_01202000.json
    ```

11. Write a test; see [TestUrjanetWataugaTransformer](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/tests/test_urjanet_watauga_transformer.py)


## Add your new scraper to the job management tool / launch script

1. Open the [launch script](../launch.py).

2. Using City of Watauga as an example, add a function similar to `watauga_ingest_batch` and a new key to the
 `scraper_functions` map.

## Test your scraper

1. Use the script `create_data_sources.py` to configure a meter in your dev setup to use your new datasource. 
    Lookup the account oid in postgres with `select oid from snapmeter_account where hex_id='5661eab691f1a2508278c01d'`.
    This returns the meter data source oid you'll need to run the scraper. In the example below, we add 
    `watauga-urjanet` to Snapmeter Account 999, Meter 4505071289158471.

    ```python scripts/create_data_sources.py 999 4505071289158471 watauga-urjanet city-of-watauga-demo```

    Make a note of the OID for the Snapmeter Meter Data Source created in this step.

2. For most Urja scrapers you will need to update the field `utility_account_id` on the `UtilityService` record
    associated with your meter, so that the scraper will associate Urjanet bills with that meter. 
    Once you have selected a target bill in the Urjanet DB, look up the "raw account number" associated with that bill. 
    Then in the `psql` shell, update the meter to use that raw account number as the utility account ID. 
    ```
    update utility_service 
    set utility_account_id = '151009074' 
    from meter where meter.oid = 4505019811696256 
    and meter.service = utility_service.oid;
    ``` 

3. Run your scraper via the launch script: `python launch.py by-oid 29 2019-01-01 2019-12-31`. The oid is the meter data source oid; the dates are required but not used for Urjanet scrapers. This runs your scraper exactly the same way AWS batch will. If everything
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

## Finally, test in Dev

The final step is to try to accomplish the same run in our dev environment, on AWS batch.

1. Commit your changes to your branch.

2. SSH to the `energy-dev-ops` EC2 in dev (should be configured in your `~/.ssh/config`)

3. Go to `projects/datafeeds` and pull your changes in (`git pull origin master; git checkout my-branch`)

4. Build your container and push to ECR. Use the tag you selected earlier for your work (e.g. `southlake`) so that
    your image doesn't get overwritten by someone else.
    ```
    $(aws ecr get-login --no-include-email --region us-east-1)
    docker build -t gridium/datafeeds:<YOUR TAG> .
    docker tag gridium/datafeeds:latest 634855895757.dkr.ecr.us-east-1.amazonaws.com/datafeeds:<YOUR TAG>
    docker push 634855895757.dkr.ecr.us-east-1.amazonaws.com/datafeeds:<YOUR TAG>
    ```

5. Configure a meter in the dev environment to use your data source, as you did locally. 
    Run `source dev-config` in `projects/datafeeds` so that you will have the right DB hostnames/passwords
    for the dev environment. 

6. Under the "Jobs" menu, click "submit job". Give your job a unique name you'll recognize (like `southlake-test-00`),
    and use the latest job definition (to get standard configurations like DB credentials). The job queue should be
     `datafeeds-dev`. Update the command to: 
    ```
    python3 launch.py by-oid <Your Snapmeter Meter Data Source OID> 2019-01-01 2019-12-31
    ```
    You can obtain logs for your job by clicking the job ID link and looking under "attempts".
    
    If successful, you should see the same output from your local test in the logs.

    If your job fails, The "Clone Job" button can be helpful for retrying.

