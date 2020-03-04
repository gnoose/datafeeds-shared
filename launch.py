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
import tarfile

import boto

from datafeeds.common.index import index_logs

from datafeeds.datasources.austin_energy_interval import (
    datafeed as austin_energy_interval,
)
from datafeeds.datasources.pacific_power_interval import (
    datafeed as pacific_power_interval,
)
from datafeeds.datasources.smud_energyprofiler_interval import (
    datafeed as smud_energyprofiler_interval,
)

from datafeeds.scrapers.bloom_interval import datafeed as bloom_interval
from datafeeds.scrapers.engie import datafeed as engie
from datafeeds.scrapers.grovestreams import datafeed as grovestreams
from datafeeds.scrapers.heco_interval import datafeed as heco_interval
from datafeeds.scrapers.nautilus import datafeed as nautilus
from datafeeds.scrapers.nvenergy_myaccount import datafeed as nvenergy_myaccount
from datafeeds.scrapers.portland_bizportal import datafeed as portland_bizportal
from datafeeds.scrapers.powertrack import datafeed as powertrack
from datafeeds.scrapers.sce_greenbutton import datafeed as sce_greenbutton
from datafeeds.scrapers.sdge_myaccount import datafeed as sdge_myaccount
from datafeeds.scrapers.sdge_greenbutton import datafeed as sdge_greenbutton_sync
from datafeeds.scrapers.solaredge import datafeed as solaredge
from datafeeds.scrapers.solren import datafeed as solren
from datafeeds.scrapers.smart_meter_texas import datafeed as smart_meter_texas
from datafeeds.scrapers.duke.billing import datafeed as duke_billing
from datafeeds.scrapers.duke.intervals import datafeed as duke_interval
from datafeeds.scrapers.sce_react.basic_billing import (
    datafeed as sce_react_basic_billing,
)
from datafeeds.scrapers.sce_react.energymanager_billing import (
    datafeed as sce_react_energymanager_billing,
)
from datafeeds.scrapers.sce_react.energymanager_interval import (
    datafeed as sce_react_energymanager_interval,
)
from datafeeds.scrapers.saltriver.intervals import datafeed as saltriver_interval
from datafeeds.scrapers.saltriver.billing import datafeed as saltriver_billing
from datafeeds.scrapers.stem import datafeed as stem

from datafeeds.urjanet.datasource.american import datafeed as american
from datafeeds.urjanet.datasource.austin_tx import datafeed as austin
from datafeeds.urjanet.datasource.calwater import datafeed as calwater
from datafeeds.urjanet.datasource.colleyville import datafeed as colleyville
from datafeeds.urjanet.datasource.constellation import datafeed as constellation
from datafeeds.urjanet.datasource.directenergy import datafeed as directenergy
from datafeeds.urjanet.datasource.fortworth import datafeed as fortworth
from datafeeds.urjanet.datasource.fostercity import datafeed as fostercity
from datafeeds.urjanet.datasource.fpl import datafeed as fpl
from datafeeds.urjanet.datasource.generic_water import datafeed as generic_water
from datafeeds.urjanet.datasource.heco import datafeed as heco_urjanet
from datafeeds.urjanet.datasource.irvineranch import datafeed as irvineranch
from datafeeds.urjanet.datasource.ladwp_water import datafeed as ladwp_water
from datafeeds.urjanet.datasource.ladwp import datafeed as ladwp
from datafeeds.urjanet.datasource.mountainview import datafeed as mountainview
from datafeeds.urjanet.datasource.nationalgrid import datafeed as nationalgrid
from datafeeds.urjanet.datasource.nve import datafeed as nve
from datafeeds.urjanet.datasource.pge import datafeed as pge
from datafeeds.urjanet.datasource.pleasanton import datafeed as pleasanton
from datafeeds.urjanet.datasource.pse import datafeed as pse
from datafeeds.urjanet.datasource.sandiego import datafeed as sandiego
from datafeeds.urjanet.datasource.sdge import datafeed as sdge
from datafeeds.urjanet.datasource.sfpuc import datafeed as sfpuc
from datafeeds.urjanet.datasource.sjwater import datafeed as sjwater
from datafeeds.urjanet.datasource.southlake import datafeed as southlake
from datafeeds.urjanet.datasource.tricounty import datafeed as tricounty
from datafeeds.urjanet.datasource.watauga import datafeed as watauga

from datafeeds.common.typing import Status
from datafeeds import db, config
from datafeeds.models import (
    SnapmeterMeterDataSource as MeterDataSource,
    SnapmeterAccount,
    SnapmeterAccountMeter,
    SnapmeterAccountDataSource as AccountDataSource,
    Meter,
    UtilityService,
)

log = logging.getLogger("datafeeds")

# Look up scraper function according to the Meter Data Source name recorded in the database.
scraper_functions = {
    "american-urjanet": american,
    "austin-energy-interval": austin_energy_interval,
    "austin-urjanet": austin,
    "bloom": bloom_interval,
    "cal-water-urjanet": calwater,
    "colleyville-water-urjanet": colleyville,
    "constellation": constellation,
    "directenergy-urjanet": directenergy,
    "duke-energy-billing": duke_billing,
    "duke-energy-interval": duke_interval,
    "engie": engie,
    "fortworth-water-urjanet": fortworth,
    "grovestreams": grovestreams,
    "fostercity-water-urjanet": fostercity,
    "fpl-urjanet": fpl,
    "generic-urjanet-water": generic_water,
    "heco-interval": heco_interval,
    "heco-urjanet": heco_urjanet,
    "irvineranch-water-urjanet": irvineranch,
    "ladwp-water-urjanet": ladwp_water,
    "ladwp-urjanet": ladwp,
    "mountainview-urjanet": mountainview,
    "nationalgrid-urjanet": nationalgrid,
    "nve-urjanet": nve,
    "nautilus": nautilus,
    "nve-myaccount": nvenergy_myaccount,
    "pacific-power-interval": pacific_power_interval,
    "pge-urjanet-v2": pge,
    "pleasanton-urjanet": pleasanton,
    "portland-bizportal": portland_bizportal,
    "powertrack": powertrack,
    "pse-urjanet": pse,
    "saltriver.billing": saltriver_billing,
    "saltriver.interval": saltriver_interval,
    "sandiego-water-urjanet": sandiego,
    "sdge-myaccount": sdge_myaccount,
    "sdge-urjanet": sdge,
    "sdge-green-button-sync": sdge_greenbutton_sync,
    "sfpuc-water-urjanet": sfpuc,
    "sce-green-button": sce_greenbutton,
    "sce.react_basic_billing": sce_react_basic_billing,
    "sce.react_energymanager_interval": sce_react_energymanager_interval,
    "sce.react_energymanager_billing": sce_react_energymanager_billing,
    "sj-water-urjanet": sjwater,
    "smart-meter-texas": smart_meter_texas,
    "smud-energyprofiler-interval": smud_energyprofiler_interval,
    "stem": stem,
    "solaredge": solaredge,
    "solren": solren,
    "southlake-urjanet": southlake,
    "tricounty-urjanet": tricounty,
    "watauga-urjanet": watauga,
}


def cleanup_workdir():
    try:
        subprocess.check_output(
            "rm -rf *",
            stderr=subprocess.STDOUT,
            cwd=config.WORKING_DIRECTORY,
            shell=True,
        )
    except subprocess.CalledProcessError as e:  # noqa E722
        log.error(
            "Failed to clear working directory %s. Output: %s",
            config.WORKING_DIRECTORY,
            e.output,
        )


def archive_run(task_id: str):
    """Write the files acquired during the scraper run to S3."""

    # Copy the scraper process log into the archive bundle if available."""
    if os.path.isfile(config.LOGPATH):
        dest = os.path.join(config.WORKING_DIRECTORY, config.DATAFEEDS_LOG_NAME)
        shutil.copy(config.LOGPATH, dest)

    tarball = "{0}.tar.gz".format(config.WORKING_DIRECTORY)
    with tarfile.open(tarball, "w:gz") as f:
        f.add(config.WORKING_DIRECTORY)

    try:
        s3conn = boto.connect_s3()
        bucket = s3conn.get_bucket(config.ARTIFACT_S3_BUCKET, validate=True)

        s3key = bucket.new_key(task_id)
        s3key.set_contents_from_filename(tarball)

        log.info(
            "Successfully uploaded archive %s to S3 bucket %s.",
            task_id,
            config.ARTIFACT_S3_BUCKET,
        )
    except:  # noqa E722
        log.exception(
            "Failed to upload archive %s to S3 bucket %s.",
            task_id,
            config.ARTIFACT_S3_BUCKET,
        )
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
        log.error(
            'No scraping procedure associated with the identifier "%s". Aborting',
            mds.name,
        )
        sys.exit(1)

    parameters = {
        "data_start": start.strftime("%Y-%m-%d"),
        "data_end": end.strftime("%Y-%m-%d"),
    }

    task_id = os.environ.get("AWS_BATCH_JOB_ID", str(uuid.uuid4()))

    log.info("Scraper Launch Settings:")
    log.info("Enabled Features: %s", config.FEATURE_FLAGS)
    log.info("Meter Data Source OID: %s", mds.oid)
    log.info("Meter: %s (%s)", meter.name, meter.oid)
    log.info("Scraper: %s", mds.name)
    log.info("Date Range: %s - %s", start, end)
    log.info("Task ID: %s", task_id)
    log.info(
        "Elasticsearch Host/Port: %s : %s",
        config.ELASTICSEARCH_HOST,
        config.ELASTICSEARCH_PORT,
    )
    log.debug(
        "Elasticsearch Credentials: %s : %s",
        config.ELASTICSEARCH_USER,
        config.ELASTICSEARCH_PASSWORD,
    )
    log.info("Platform Host/Port: %s : %s", config.PLATFORM_HOST, config.PLATFORM_PORT)

    cleanup_workdir()
    try:
        status = scraper_fn(account, meter, mds, parameters, task_id=task_id)

        if config.enabled("ES_INDEX_LOGS"):
            index_logs(task_id, account, meter, mds, status)
        if config.enabled("S3_ARTIFACT_UPLOAD"):
            archive_run(task_id)
    except:  # noqa=E722
        log.exception("The scraper run has failed due to an unhandled exception.")
        status = Status.FAILED

    db.session.commit()
    db.session.close()
    sys.exit(status.value)


def launch_by_oid(meter_data_source_oid: int, start: date, end: date):
    db.init()
    _launch_meter_datasource(
        db.session.query(MeterDataSource).get(meter_data_source_oid), start, end
    )


def launch_by_meter(
    meter_oid: int, start: Optional[date], end: Optional[date], source_type: str
):
    db.init()
    if not start:
        start = date(2015, 1, 1)
    if not end:
        end = date.today()
    mds = (
        db.session.query(MeterDataSource)
        .filter_by(_meter=meter_oid)
        .filter(MeterDataSource.source_types.any(source_type))
        .first()
    )
    _launch_meter_datasource(mds, start, end)


def launch_by_name(
    scraper_id: str,
    start: date,
    end: date,
    account_id: str,
    service_id: str,
    username: Optional[str],
    password: Optional[str],
    meta: Optional[dict],
):
    db.init()
    config.FEATURE_FLAGS = (
        set()
    )  # A manual run is just for dev testing. Disable all data upload features.

    scraper_fn = scraper_functions.get(scraper_id)
    if scraper_fn is None:
        log.error(
            'No scraping procedure associated with the identifier "%s". Aborting',
            scraper_id,
        )
        sys.exit(1)

    parameters = {
        "data_start": start.strftime("%Y-%m-%d"),
        "data_end": end.strftime("%Y-%m-%d"),
    }

    log.info("Scraper Launch Settings:")
    log.info("Scraper: %s", scraper_id)
    log.info("Date Range: %s - %s", start, end)
    log.info("Metadata: %s", meta)

    # Set up the necessary objects to make a local run look like one in production.
    service = UtilityService(service_id, account_id)
    db.session.add(service)
    db.session.flush()

    meter = Meter("dummy")
    meter.service = service.oid
    db.session.add(meter)
    db.session.flush()

    account = SnapmeterAccount(name="dummy", created=datetime.now())
    db.session.add(account)
    db.session.flush()

    sam = SnapmeterAccountMeter(
        account=account.oid, meter=meter.oid, utility_account_id=account_id
    )
    db.session.add(sam)
    db.session.flush()

    ads = AccountDataSource(
        _account=account.oid, source_account_type="%s-dummy" % scraper_id, name="dummy"
    )
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

    launch_by_name(
        args.scraper_id,
        args.start,
        args.end,
        args.account_id,
        args.service_id,
        args.username,
        args.password,
        meta,
    )


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
sp_by_oid.add_argument(
    "start",
    type=_date,
    help="Start date of the range to scrape (YYYY-MM-DD, inclusive)",
)
sp_by_oid.add_argument(
    "end", type=_date, help="Final date of the range to scrape (YYYY-MM-DD, exclusive)"
)


sp_by_name = subparser.add_parser("by-name", help="...based on a Scraper name.")
sp_by_name.set_defaults(func=launch_by_name_args)
sp_by_name.add_argument(
    "scraper_id", type=str, help="Scraper name (e.g. nve-myaccount)"
)
sp_by_name.add_argument(
    "account_id", type=str, help="Utility's identifier for the account to be scraped."
)
sp_by_name.add_argument(
    "service_id", type=str, help="Utility's identifier for the meter to be scraped."
)
sp_by_name.add_argument(
    "start",
    type=_date,
    help="Start date of the range to scrape (YYYY-MM-DD, inclusive)",
)
sp_by_name.add_argument(
    "end", type=_date, help="Final date of the range to scrape (YYYY-MM-DD, exclusive)"
)
sp_by_name.add_argument("--username", type=str, help="Username for utility login.")
sp_by_name.add_argument("--password", type=str, help="Password for utility login.")
sp_by_name.add_argument(
    "--meta",
    type=str,
    help='Additional scraper parameters in a JSON blob. (e.g. {"foo": "bar"}',
)


sp_by_oid = subparser.add_parser(
    "by-meter", help="...based on a Meter OID and source type"
)
sp_by_oid.set_defaults(func=launch_by_meter_args)
sp_by_oid.add_argument("oid", type=int, help="Meter OID.")
sp_by_oid.add_argument("source_type", type=str, help="billing or interval")
sp_by_oid.add_argument(
    "--start",
    type=_date,
    help="Start date of the range to scrape (YYYY-MM-DD, inclusive)",
)
sp_by_oid.add_argument(
    "--end",
    type=_date,
    help="Final date of the range to scrape (YYYY-MM-DD, exclusive)",
)


def main():
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
