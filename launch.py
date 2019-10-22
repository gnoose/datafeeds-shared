import argparse
import logging
import os
import sys
import uuid
from argparse import Namespace
from datetime import datetime, date

from datafeeds import db
from datafeeds.datasources import southlake, watauga
from datafeeds.models import SnapmeterMeterDataSource as MeterDataSource


log = logging.getLogger("datafeeds")


# Look up scraper function according to the Meter Data Source name recorded in the database.
scraper_functions = {
    "watauga-urjanet": watauga.datafeed,
    "southlake-urjanet": southlake.datafeed,
}


def launch_by_oid(meter_data_source_oid: int, start: date, end: date):
    mds = db.session.query(MeterDataSource).get(meter_data_source_oid)

    if mds is None:
        log.error("No data source associated with OID %s. Aborting.", meter_data_source_oid)
        sys.exit(1)

    account = None
    if mds.account_data_source is not None:
        ads = mds.account_data_source
        account = ads.account

    meter = mds.meter

    scraper_fn = scraper_functions.get(mds.name)

    if scraper_fn is None:
        log.error("No scraping procedure associated with the identifier \"%s\". Aborting", mds.name)
        sys.exit(1)

    parameters = {
        "interval_start": start.strftime("%Y-%m-%d"),
        "interval_end": end.strftime("%Y-%m-%d")
    }

    task_id = os.environ.get("AWS_BATCH_JOB_ID", uuid.uuid4())

    log.info("Scraper Launch Settings:")
    log.info("Meter Data Source OID: %s", meter_data_source_oid)
    log.info("Meter: %s (%s)", meter.name, meter.oid)
    log.info("Scraper: %s", mds.name)
    log.info("Date Range: %s - %s", start, end)
    log.info("Task ID: %s", task_id)

    scraper_fn(account, meter, mds, parameters, task_id=task_id)


def launch_by_oid_args(args: Namespace):
    launch_by_oid(args.oid, args.start, args.end)


def _date(d):
    return datetime.strptime(d, "%Y-%m-%d").date()


parser = argparse.ArgumentParser(description="Launch a scraper")
subparser = parser.add_subparsers(dest="how")
subparser.required = True

sp_by_oid = subparser.add_parser("by-oid", help="...based on a Meter Data Source OID.")
sp_by_oid.set_defaults(func=launch_by_oid_args)
sp_by_oid.add_argument("oid", type=int, help="Snapmeter Meter Data Source OID.")
sp_by_oid.add_argument("start", type=_date, help="Start date of the range to scrape (YYYY-MM-DD, inclusive)")
sp_by_oid.add_argument("end", type=_date, help="Final date of the range to scrape (YYYY-MM-DD, exclusive)")


def main():
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    db.init()
    main()
    sys.exit(0)

