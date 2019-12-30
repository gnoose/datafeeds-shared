import argparse
from argparse import Namespace
from datetime import datetime, date
import json
import logging
import os
import shutil
import subprocess
import sys
from typing import Optional
import uuid
import zipfile

import boto3

from datafeeds.common.typing import Status
from datafeeds import db, config
from datafeeds import datasources
from datafeeds.models import \
    SnapmeterMeterDataSource as MeterDataSource, \
    SnapmeterAccount, SnapmeterAccountMeter, \
    SnapmeterAccountDataSource as AccountDataSource, \
    Meter, UtilityService

log = logging.getLogger("datafeeds")


# Look up scraper function according to the Meter Data Source name recorded in the database.
scraper_functions = {
    "american-urjanet": datasources.american.datafeed,
    "austin-energy-interval": datasources.austin_energy_interval.datafeed,
    "austin-urjanet": datasources.austin_tx.datafeed,
    "heco-interval": datasources.heco_interval.datafeed,
    "heco-urjanet": datasources.heco.datafeed,
    "mountainview-urjanet": datasources.mountainview.datafeed,
    "nve-myaccount": datasources.nvenergy_myaccount.datafeed,
    "pacific-power-interval": datasources.pacific_power_interval.datafeed,
    "pleasanton-urjanet": datasources.pleasanton.datafeed,
    "sce-green-button": datasources.sce_greenbutton.datafeed,
    "smud-energyprofiler-interval": datasources.smud_interval.datafeed,
    "solaredge": datasources.solaredge.datafeed,
    "solren": datasources.solren.datafeed,
    "southlake-urjanet": datasources.southlake.datafeed,
    "watauga-urjanet": datasources.watauga.datafeed,
}


def cleanup_workdir():
    try:
        subprocess.check_output("rm -rf *", stderr=subprocess.STDOUT, cwd=config.WORKING_DIRECTORY, shell=True)
    except subprocess.CalledProcessError as e:  # noqa E722
        log.error("Failed to clear working directory %s. Output: %s", config.WORKING_DIRECTORY, e.output)


def archive_run(task_id: str):
    """Write the files acquired during the scraper run to S3."""

    # Copy the scraper process log into the archive bundle if available."""
    logpath = os.path.join(config.DATAFEEDS_ROOT, config.DATAFEEDS_LOG_NAME)
    if os.path.isfile(logpath):
        dest = os.path.join(config.WORKING_DIRECTORY, config.DATAFEEDS_LOG_NAME)
        shutil.copy(logpath, dest)

    zip_filename = "{}.zip".format(task_id)
    archive_path = os.path.join(config.WORKING_DIRECTORY, zip_filename)

    try:
        zipf = zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED)
        for root, dirs, files in os.walk(config.WORKING_DIRECTORY):
            for filename in files:
                if filename != zip_filename:
                    archive_name = os.path.join(root, filename) \
                        .replace(config.WORKING_DIRECTORY, task_id)
                    zipf.write(os.path.join(root, filename), arcname=archive_name)
        zipf.close()
    except:  # noqa E722
        log.exception("Failed to compress artifacts in archive %s.", zip_filename)
        raise

    try:
        boto3.resource("s3").meta.client.upload_file(archive_path, config.ARTIFACT_S3_BUCKET, zip_filename)
        log.info("Successfully uploaded archive %s to S3 bucket %s.", archive_path, config.ARTIFACT_S3_BUCKET)
    except:  # noqa E722
        log.exception("Failed to upload archive %s to S3 bucket %s.", zip_filename, config.ARTIFACT_S3_BUCKET)
        raise


def _launch_meter_datasource(mds: MeterDataSource, start: date, end: date):
    if mds is None:
        log.error("No data source. Aborting.")
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
        "data_start": start.strftime("%Y-%m-%d"),
        "data_end": end.strftime("%Y-%m-%d")
    }

    task_id = os.environ.get("AWS_BATCH_JOB_ID", str(uuid.uuid4()))

    log.info("Scraper Launch Settings:")
    log.info("Enabled Features: %s", config.FEATURE_FLAGS)
    log.info("Meter Data Source OID: %s", mds.oid)
    log.info("Meter: %s (%s)", meter.name, meter.oid)
    log.info("Scraper: %s", mds.name)
    log.info("Date Range: %s - %s", start, end)
    log.info("Task ID: %s", task_id)
    log.info("Elasticsearch Host/Port: %s : %s", config.ELASTICSEARCH_HOST, config.ELASTICSEARCH_PORT)
    log.debug("Elasticsearch Credentials: %s : %s", config.ELASTICSEARCH_USER, config.ELASTICSEARCH_PASSWORD)
    log.info("Platform Host/Port: %s : %s", config.PLATFORM_HOST, config.PLATFORM_PORT)

    cleanup_workdir()
    try:
        status = scraper_fn(account, meter, mds, parameters, task_id=task_id)
        if config.enabled("S3_ARTIFACT_UPLOAD"):
            archive_run(task_id)
    except:  # noqa=E722
        log.exception("The scraper run has failed due to an unhandled exception.")
        status = Status.FAILED

    db.session.close()
    sys.exit(status.value)


def launch_by_oid(meter_data_source_oid: int, start: date, end: date):
    db.init()
    _launch_meter_datasource(
        db.session.query(MeterDataSource).get(meter_data_source_oid),
        start,
        end)


def launch_by_meter(meter_oid: int, start: Optional[date], end: Optional[date], source_type: str):
    db.init()
    if not start:
        start = date(2015, 1, 1)
    if not end:
        end = date.today()
    mds = db.session.query(MeterDataSource).\
        filter_by(_meter=meter_oid).\
        filter(MeterDataSource.source_types.any(source_type)).\
        first()
    _launch_meter_datasource(mds, start, end)


def launch_by_name(scraper_id: str,
                   start: date, end: date,
                   account_id: str, service_id: str,
                   username: Optional[str], password: Optional[str], meta: Optional[dict]):
    db.init()
    config.FEATURE_FLAGS = set()  # A manual run is just for dev testing. Disable all data upload features.

    scraper_fn = scraper_functions.get(scraper_id)
    if scraper_fn is None:
        log.error("No scraping procedure associated with the identifier \"%s\". Aborting", scraper_id)
        sys.exit(1)

    parameters = {
        "data_start": start.strftime("%Y-%m-%d"),
        "data_end": end.strftime("%Y-%m-%d")
    }

    log.info("Scraper Launch Settings:")
    log.info("Scraper: %s", scraper_id)
    log.info("Date Range: %s - %s", start, end)
    log.info("Metadata: %s", meta)

    # Set up the necessary objects to make a local run look like one in production.
    service = UtilityService(service_id)
    db.session.add(service)
    db.session.flush()

    meter = Meter("dummy")
    meter.service = service.oid
    db.session.add(meter)
    db.session.flush()

    account = SnapmeterAccount(name="dummy", created=datetime.now())
    db.session.add(account)
    db.session.flush()

    sam = SnapmeterAccountMeter(account=account.oid, meter=meter.oid, utility_account_id=account_id)
    db.session.add(sam)
    db.session.flush()

    ads = AccountDataSource(_account=account.oid, source_account_type="%s-dummy" % scraper_id, name="dummy")
    ads.encrypt_username(username)
    ads.encrypt_password(password)
    db.session.add(ads)
    db.session.flush()

    mds = MeterDataSource(name=scraper_id, meta=meta)
    mds.meter = meter
    mds.account_data_source = ads
    db.session.add(mds)
    db.session.flush()

    cleanup_workdir()
    try:
        status = scraper_fn(account, meter, mds, parameters)
    except:  # noqa=E722
        log.exception("The scraper run has failed due to an unhandled exception.")
        status = Status.FAILED

    db.session.rollback()  # Don't commit fake objects to the database.
    db.session.close()
    sys.exit(1 if status == Status.FAILED else 0)


def launch_by_oid_args(args: Namespace):
    launch_by_oid(args.oid, args.start, args.end)


def launch_by_name_args(args: Namespace):
    if args.meta:
        meta = json.loads(args.meta)
    else:
        meta = None

    launch_by_name(args.scraper_id,
                   args.start, args.end,
                   args.account_id, args.service_id,
                   args.username, args.password, meta)


def launch_by_meter_args(args: Namespace):
    launch_by_meter(args.oid, args.start, args.end, args.source_type)


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


sp_by_name = subparser.add_parser("by-name", help="...based on a Scraper name.")
sp_by_name.set_defaults(func=launch_by_name_args)
sp_by_name.add_argument("scraper_id", type=str, help="Scraper name (e.g. nve-myaccount)")
sp_by_name.add_argument("account_id", type=str, help="Utility's identifier for the account to be scraped.")
sp_by_name.add_argument("service_id", type=str, help="Utility's identifier for the meter to be scraped.")
sp_by_name.add_argument("start", type=_date, help="Start date of the range to scrape (YYYY-MM-DD, inclusive)")
sp_by_name.add_argument("end", type=_date, help="Final date of the range to scrape (YYYY-MM-DD, exclusive)")
sp_by_name.add_argument("--username", type=str, help="Username for utility login.")
sp_by_name.add_argument("--password", type=str, help="Password for utility login.")
sp_by_name.add_argument("--meta", type=str,
                        help="Additional scraper parameters in a JSON blob. (e.g. {\"foo\": \"bar\"}")


sp_by_oid = subparser.add_parser("by-meter", help="...based on a Meter OID and source type")
sp_by_oid.set_defaults(func=launch_by_meter_args)
sp_by_oid.add_argument("oid", type=int, help="Meter OID.")
sp_by_oid.add_argument("source_type", type=str, help="billing or interval")
sp_by_oid.add_argument("--start", type=_date, help="Start date of the range to scrape (YYYY-MM-DD, inclusive)")
sp_by_oid.add_argument("--end", type=_date, help="Final date of the range to scrape (YYYY-MM-DD, exclusive)")


def main():
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
