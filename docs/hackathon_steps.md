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

1. A copy of the Urjanet database. SFTP to the ops machine and download /var/builds/urjanet_dumps/urjanet.sql.

2. The urjanet password so that you can download bill PDFs. (Share this the day of the hackathon.)

3. A docker-compose setup. Run the Urjanet [setup steps](./urjanet_setup.md) and PostgreSQL 
[setup steps](./database_setup.md).

# Steps

## Set up a new Urjanet Scraper

1. Set up a new branch for your work. Choose a tag (e.g. City of Watauga Water -> `watauga`).

2. Log into you Urjanet database and look up some bills in the `Accounts` table. Obtain bill PDFs for reference.
    You may want to write down a table of the bill period dates, cost, use, peak, and account numbers you found,
    for reference.

3. We has some CLI tools for building the typical parts of an Urjanet scraper. Pair up with someone who has used these
    before, for a quick tutorial. You'll need to set up a new datasource and transformer for your utility.
    
4. Using your local Urjanet DB, prepare your test data and confirm your test matches the bill data you chose in step #2.

## Add your new scraper to the job management tool / launch script.

1. Open the [launch script](../launch.py).

2. Using City of Watauga as an example, add a function similar to `watauga_ingest_batch` and a new key to the
 `scraper_functions` map.

## Test your scraper.

1. Use the script `create_data_sources.py` to configure a meter in your dev setup to use your new datasource. 
In the example below, we add `watauga-urjanet` to Snapmeter Account 999, Meter 4505071289158471.

    ```python scripts/create_data_sources.py 999 4505071289158471 watauga-urjanet city-of-watauga-demo```

2. Run your scraper via the launch script. This runs your scraper exactly the same way AWS batch will. If everything
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
