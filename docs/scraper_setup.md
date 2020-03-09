# Writing a new scraper

## Prerequisites

Make sure you have:

1. A python 3.6 environment:

    ```
    pyenv virtualenv 3.6.4 datafeeds
    pyenv activate datafeeds
    pip install -r requirements.txt
    pip install flake8
    ```
2. A copy of the Urjanet database (warning: 1.1GB unzipped). `scp ops:/builds/urjanet_dumps/urjanet.sql.gz .`

3. The urjanet password so that you can download bill PDFs. (from ops)

4. A docker-compose setup. Run the Urjanet [setup steps](./urjanet_setup.md) and PostgreSQL
[setup steps](./database_setup.md).


## Set up a new Urjanet scraper

(see also: [Urjanet docs](./urjanet.md))

1. Set up a new branch for your work. Choose a tag (e.g. City of Watauga Water -> `watauga`).

2. From the top level of your `datafeeds` repo, run `export PYTHONPATH=$(pwd):$PYTHONPATH`

3. Review bills for the utility you're working on
    - go to the [Production Urjanet Explorer sheet](https://docs.google.com/spreadsheets/d/1qJcgRpf7BgdhgWHE2Nd-HE0w4vyk3a0NbnoLTrBR2CM/edit#gid=240288574)
    - find the tab for the utility
    - click the pdf link to download a view a PDF of the bill (requires Urjanet password)

4. Log into your Urjanet database and look up some bills in the `Accounts` table (AccountNumber from the Account Number column in the sheet). You may want to write down a table of the bill period dates, cost, use, peak, and account numbers you found, for reference.

    ```
    select c.IntervalStart, c.IntervalEnd, c.ChargeAmount, ChargeUnitsUsed, a.UtilityProvider
    from Charge c, Meter m, Account a
    where a.AccountNumber='07292000' and a.PK=c.AccountFK and a.PK=m.AccountFK and m.PK=c.MeterFK;
    ```

5. Write a datasource class in `datafeeds/urjanet/datasource` for the scraper to load data from Urjanet. See [WataugaDatasource](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/datasource/watauga.py).

6. Write a transformer class in `datafeeds/urjanet/transformer` for the scraper to adjust any unusual cases in the Urjanet data. See [WataugaTransformer](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/transformer/watauga.py).

7. Add your new utility to [cli_hooks.py](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/scripts/cli_hooks.py)

8. Create a data dump for your test account(s). Review and compare to PDF version of bills.

    ```
    cd datafeeds/urjanet/scripts
    mkdir ../tests/data/watauaga
    python dump_urja_json.py watauga 07292000 > ../tests/data/watauga/input_07292000.json
    ```

10. Run your transformer on the extracted data. Review and compare to PDF version of bills.

    ```
    python transform_urja_json.py ../tests/data/watauga/input_07292000.json watauga > ../tests/data/watauga/expected_07292000.json
    ```

11. Write a test; see [TestUrjanetWataugaTransformer](https://github.com/Gridium/datafeeds/blob/master/datafeeds/urjanet/tests/test_urjanet_watauga_transformer.py)


## Add your new scraper to the job management tool / launch script

1. Open the [launch script](../launch.py).

2. Using City of Watauga as an example, add a function similar to `watauga_ingest_batch` and a new key to the
 `scraper_functions` map. This is the same key used in the [UI login list](https://github.com/Gridium/snapmeter/blob/master/frontend/main/app/models/utility-login.js#L46).

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
    Then in the `psql` shell, update the meter to use that raw account number as the utility account ID:
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
    2019-10-02 17:49:07,987 : INFO :    scrape_bills: True
    2019-10-02 17:49:07,987 : INFO :    scrape_readings: False
    2019-10-02 17:49:07,987 : INFO :    urja_datasource: <datafeeds.urjanet.datasource.watauga.WataugaDatasource object at 0x10eeaa6d8>
    2019-10-02 17:49:07,987 : INFO :    urja_transformer: <datafeeds.urjanet.transformer.watauga.WataugaTransformer object at 0x10eebc5c0>
    2019-10-02 17:49:07,987 : INFO :    utility_name: city-of-watauga
    2019-10-02 17:49:07,987 : INFO :    fetch_attachments: True
    2019-10-02 17:49:08,252 : INFO : ================================================================================
    2019-10-02 17:49:08,252 : INFO : Final Billing Summary --- This is what would have been uploaded to platform.
    2019-10-02 17:49:08,252 : INFO : ================================================================================
    2019-10-02 17:49:08,253 : INFO : Start       End         Cost        Use         Peak       Has PDF
    2019-10-02 17:49:08,253 : INFO : 2019-07-11  2019-08-14  1972.73     4810.0      None       False
    2019-10-02 17:49:08,253 : INFO : 2019-08-15  2019-09-12  2396.16     9030.0      None       False
    2019-10-02 17:49:08,253 : INFO : ================================================================================
    ```

## Deploy to production

After creating a PR, getting it reviewed, and merging, you're ready to deploy.

From ops,

```
source venv/bin/activate
export PYTHONPATH=$(pwd)
source ops/deploy.env
python ops/deploy.py
```

See [deploy directions](./deploy.md) for more details.

## Test your new scraper

Merge and deploy the changes to add your scraper to the admin UI.

Add your new scraper to a test meter in admin (admin / Scraper staging / meter page / Scrapers / Add new).
For Urjanet scrapers, set the utility account id (edit meter / Account ID) to an
Account Number from the
[Production Urjanet Explorer sheet](https://docs.google.com/spreadsheets/d/1qJcgRpf7BgdhgWHE2Nd-HE0w4vyk3a0NbnoLTrBR2CM/edit).

Then run your scraper from ops:

    ./build.sh
    ./run.sh by-meter meterOid billing|interval


Once testing is complete, enable the scraper for the scheduler by updating the database record:

    update datafeeds_feed_config set enabled=TRUE where name='launch_key_here';

Finally, announce in the `#scrapers` Slack channel that the new scraper is ready for use.

### stasis transaction backup

(remove this section once we get rid of stasis transactions)

If readings or bills don't appear within a few minutes, stasis transactions may be backed up.
To verify, run `select oid, target, status from stasis_transaction where target=meterOid` on the
production database. If the `status` is `verifying`, you can jump the queue by posting a message
to the immediate queue. Starting from ops:

    ssh platform2.gridium.prod
    cd ~/groovy/analytics

Edit `bifrost-immediate.groovy` to set `meterOid` to your meter, and `transactionOid` to the stasis_transaction.oid from above. Then,

    morph
    groovy analytics/bifrost-immediate.groovy

This will put a message on the immediate queue, which should go through within a few minutes to unfreeze your data.
