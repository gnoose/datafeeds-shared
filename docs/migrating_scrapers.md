# Migrating Scrapers from Tasks

This document describes how to move a scraper from `tasks` to `datafeeds`.

# Motivation

As we overhaul our ETL system, I think it's important to keep our end-goals in mind.

- Batch should make it easier to test scrapers.
- We want a system that has no code dependencies on `webapps`.
- We'd like to have a more robust dev environment that closely matches production.
- We want AWS to manage the hardest parts of job scheduling and status tracking for us.
- Less boilerplate in our ETL repo (though some is unavoidable).

# Steps

Tips:
- This [pull request](https://github.com/Gridium/datafeeds/pull/11/files) shows how we migrated an interval scraper. 
    It may be helpful as a model of what your end result will look like.
- I highly recommend using PyCharm or some other IDE with static analysis capabilities for this work. 
    If you don't have the ability to quickly search the tasks and datafeeds repos for particular modules and symbols,
    your progress will (likely) be very slow.

Below is my process for moving a scraper.

## Move code from Tasks

1. Look in `gridium_tasks/data_sources/scrapers` for a module named after your utility. 
There should be a file called `task.py` or similar, containing a celery task definition.

2. Inside `datafeeds/datasources`, create a file named after your utility that defines a function called `datafeed`.
This function plays a role similar to the celery task definition; namely, it creates whatever configuration object
is needed to launch the API/Web Scraper code. The body of the `datafeed` function should be almost the same as the 
celery task you are replacing. Use `run_datafeed` instead of `launch`.

3. At this point, the IDE is likely flagging several errors, because the scraper and configuration objects are not
defined in the repo yet. This is good; all that remains to do now is fix each undefined value/object/function error.

4. In your original celery task definition, there is likely an import like 
    ```
    import gridium_tasks.lib.scrapers.nvenergy_myaccount.interval as nve
    ``` 
    Create a corresponding module in `datafeeds/scrapers`. For each `gridium_tasks` import, there should already be
    an analogous module existing in `datafeeds` for you to use. The IDE can helpfully suggest what to import from
    datafeeds. Dependencies not authored by Gridium can be added to `requirements.txt`.

## Add your scraper to the launch script.

Open `launch.py` and add your new datafeed function to the `scraper_functions` dictionary. The key that you use *must*
be the same as the `name` field attached to `SnapmeterMeterDataSource` records in production, or we won't be able to
 dispatch scraper jobs correctly. 
 
Example, the NVEnergy celery task looks like this:

```
import celery

from gridium_tasks.data_sources import decorators
import gridium_tasks.lib.scrapers.nvenergy_myaccount.interval as nve
from gridium_tasks.lib.scrapers.utils import launch

NAME = "nve-myaccount"


@celery.task(
    name="%s.ingest" % NAME,
    bind=True,
    max_retries=2
)
@decorators.datasource
def scrape(self, account, meter, datasource, params):
    configuration = nve.NvEnergyMyAccountConfiguration(
        account_id=datasource["utility_account_id"],
        meter_id=meter["service_id"]
    )
    launch(
        nve.NvEnergyMyAccountScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=self.request.id
    )

```

and the correct key is `nve-myaccount`.

## Set Up for Testing

1. Identify a production meter that uses the scraper you plan to move.

2. Clone that meter's building, account ID, and service ID information to create a meter in the
 "Scraper Staging" account or the dev environment.
 
3. Make a note of the credentials the scraper uses (from the "utility logins" tab in admin).

4. Find the logs from an existing scraper run, so that you know what output to expect when the scraper succeeds.


## Test your scraper.

You should be able to run your scraper locally like this:

```
launch.py by-name <Utility Key> <Account ID> <SAID> <Start Date> <End Date> --username="<username>" --password="<password>"
```

In a dev/staging environment, you should also test your scraper against a test meter data source, like this:

```
./build.sh
./run.sh by-oid 115769 2019-01-01 2019-12-31
```


