# Creating a new Urjanet scraper

## Collect bill data

Goal: get bill data in a structured format, for use in testing a new scraper.

### Get bills and PDFs from Urjanet database

Run [scripts/prep_urja_bills.py](../scripts/prep_urja_bills.py) *UtilityProvider*

This will:

  - load Urjanet data from your local mysql database for the specified utility
  - download bill PDFs to **UtilityProvider** directory
  - create **UtilityProvider**/bills.csv file with one line per bill

Sample bills.csv

    utility,utility_account_id,service_id,start,end,cost,used,peak,filename,statement
    EBMUD,63507723958,02002405,,,,,,1eb40610-6fcd-d408-bbb0-0e3eb1fe9485.pdf,2020-12-16
    EBMUD,63507723958,02002405,,,,,,1eb3e0e2-fb24-daa3-877e-0e0bcebc926b.pdf,2020-11-13

### Outsource collecting bill data from PDFs

Create a directory for the new utility in
[Gridium > Scraper development](https://drive.google.com/drive/u/0/folders/1Ze-ByPO9-331IYaWyXoZpTN_rDzbyUem)

Upload bills PDFs.

Copy [Data capture instructions](https://docs.google.com/document/d/1k_H6NvkpA8uLljpm9EG4oTCTk9DEGLXzPthOX8EbV6I/edit)
to new utility directory. Mark up a bill for the new utility with where to find
cost, used, peak, start, and end dates. Use this to update Data capture instructures.

Copy data entry worksheets (ie [EBMUD worksheet 1, EBMUD worksheet 2](https://drive.google.com/drive/folders/10zMqgJnAMGagDF65hEx35Qe7BCi9bZhm)) to new utility directory.
Import bills.csv to populate with data to be filled in.

Outsource the job to collect data.

When data comes in, copy it to the utility sheet and verify. If dates overlap, move the end date back one day.
Format numeric values without commas.

Export collected data to datafeeds/urjanet/tests/data/utility-id.csv. This data will be used as the
expected values for the scraper test.

## Write scraper

### Create classes

Run [scripts/setup_urja.py](../scripts/setup_urja.py) classes *utility-id* *UtilityProvider*

    python setup_urja.py classes ebmud EBMUD --water

This will:
  - create a datasource class to load data
  - create a (mostly empty) transformer class to make any required updates (ie converting units, adjusting dates)
  - create a test that compares fixture data with the transformer results

### Create test data

Run scripts from the `scripts` directory.

Export expected values sheet to csv, and copy to datafeeds/urjanet/tests/data/*utility-id*.csv

Run [scripts/setup_urja.py](../scripts/setup_urja.py) tests *utility-id* *UtilityProvider*

    python setup_urja.py classes ebmud EBMUD --water

This will:
### Outsource scraper job

Add comments to transformer class describing what needs to be done.

[Setup a pull request](contractors.md) and outsource the job. Copy setup instructions from
[another Urja scraper PR](https://github.com/Gridium/datafeeds-shared/pull/24).

## Integrate

Review the code, then merge.

Run the scraper on provisioned meters.

