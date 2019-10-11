import argparse
from argparse import Namespace
from datetime import datetime, date, timedelta
import logging
import sys
import os
import uuid
from typing import Optional

from dateutil import parser as dateparser

from datafeeds import db, config
from datafeeds.common.support import Credentials, DateRange
from datafeeds.common.typing import BillingData, show_bill_summary
from datafeeds.common import index
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
import datafeeds.urjanet.datasource as urjanet_datasource
import datafeeds.urjanet.transformer as urjanet_transformer
from datafeeds.urjanet.transformer.base import UrjanetGridiumTransformer
from datafeeds.urjanet.scraper import BaseUrjanetScraper, BaseUrjanetConfiguration
from datafeeds.models import Meter, SnapmeterAccount, \
    SnapmeterMeterDataSource as MeterDataSource, \
    SnapmeterAccountDataSource as AccountDataSource


log = logging.getLogger("datafeeds")


def scraper_dates(start_iso, end_iso):
    if isinstance(start_iso, date):
        start = start_iso
    elif start_iso:
        start = dateparser.parse(start_iso).date()
    else:
        start = date(2000, 1, 1)

    if isinstance(end_iso, date):
        end = end_iso
    elif end_iso:
        end = dateparser.parse(end_iso).date()
    else:
        end = date.today() - timedelta(days=1)

    if start and end <= start:
        end = start + timedelta(days=1)

    return start, end


def batch_launch(scraper_class, account: SnapmeterAccount, meter: Meter,
                 datasource: MeterDataSource, params: dict, configuration=None,
                 task_id=None, transforms=None):
    """
    Convenience function for launching a scraper and uploading resulting readings and/or bills.
    datasource is a serialized SnapmeterMeterDataSource, with credentials

    Not all scrapers need access to stored utility account/service IDs,
    so pass those through via a configuration object if needed.

    Managing ETL logger is done via ETL pipeline so is not necessary here.

    If task_id is specified, synchronously update ES record with data range.

    This is slightly different than the standard launch to make it work better in the AWS
    Batch environment:
      - meter, account, and datasource are SQLAlchemy objects, not Mongo data (TODO: replace
        webapps import with task-local versions)
      - index start and end events (was in celeryconfig for tasks)
      - run indexing synchronously, instead of starting a celery task
      - POST data directly to platform, instead of a webapps endpoint that POSTs to platform
    """

    def upload_readings(readings):
        # FIXME: Enable interval data upload in dev environment (no platform API available there).
        # if transforms and readings:
        #     readings = interval_transform.transform(
        #         transforms, task_id, scraper_class.__name__, meter.oid, readings)
        # interval_uploader.upload_data(readings, account.hex_id, meter.oid)
        # if task_id:
        #     index.update_readings_range(task_id, meter.oid, readings)
        log.info("Final Interval Summary --- This is what would have been uploaded to platform.")
        for when, intervals in readings.items():
            log.info("%s: %s intervals." % (when, len(intervals)))
        return

    def upload_bills(billing_data: BillingData):
        # FIXME: Enable bill upload in dev environment (no platform API available there).
        # bills = []
        # for bill in billing_data:
        #     if not bill:
        #         continue
        #     bills.append({
        #         "start": bill.start.strftime("%Y-%m-%d"),
        #         "end": bill.end.strftime("%Y-%m-%d"),
        #         "cost": str(bill.cost),
        #         "used": str(bill.used) if bill.used else "0.0",
        #         "peak": str(bill.peak) if bill.peak else "0.0",
        #         "items": bill.items or [],
        #         "attachments": bill.attachments or []
        #     })
        # response = requests.post(
        #     "%s/object/utility-service/%s/bills/import" % (config.PLATFORM_API_URL, meter.utility_service.service),
        #     data={"importance": "product", "bills": bills},
        #     headers={"Content-type": "application/json", "Accept": "*"}
        # )
        # response.raise_for_status()
        # if task_id:
        #     index.update_billing_range(task_id, billing_data)
        title = "Final Billing Summary --- This is what would have been uploaded to platform."
        show_bill_summary(billing_data, title)

    date_range = DateRange(*scraper_dates(
        params.get("interval_start"),
        params.get("interval_end")
    ))

    if datasource.account_data_source:
        parent: AccountDataSource = datasource.account_data_source
        credentials = Credentials(parent.username, parent.password)
    else:
        credentials = Credentials(None, None)

    if task_id and config.enabled("ES_TASK_INDEXING"):
        index.index_etl_run(task_id, {
            "started": datetime.now(),
            "status": "STARTED",
            "accountId": account.hex_id,
            "accountName": account.name,
            "meterId": meter.oid,
            "meterName": meter.name,
            "scraper": scraper_class.__name__,
        })
    try:
        error = None
        with scraper_class(credentials, date_range, configuration) as scraper:
            scraper.scrape(readings_handler=upload_readings, bills_handler=upload_bills)
            status = "SUCCESS"
    except Exception as exc:
        log.exception("Scraper run failed.")
        status = "FAILURE"
        error = repr(exc)

    if task_id and config.enabled("ES_TASK_INDEXING"):
        index.index_etl_run(task_id, {"status": status, "error": error})


def urjanet_ingest_base(account: SnapmeterAccount, meter: Meter,
                         datasource: MeterDataSource, params: dict,
                         urja_datasource: UrjanetPyMySqlDataSource,
                         transformer: UrjanetGridiumTransformer,
                         task_id: Optional[str] = None):
    conn = db.urjanet_connection()

    try:
        urja_datasource.conn = conn
        scraper_config = BaseUrjanetConfiguration(
            urja_datasource=urja_datasource,
            urja_transformer=transformer,
            utility_name=meter.utility_service.utility,
            fetch_attachments=True
        )

        batch_launch(
            BaseUrjanetScraper,
            account,
            meter,
            datasource,
            params,
            configuration=scraper_config,
            task_id=task_id)
    finally:
        conn.close()


def watauga_ingest_batch(account: SnapmeterAccount, meter: Meter,
                         datasource: MeterDataSource, params: dict,
                         task_id: Optional[str] = None):
    """
    Get data from Urjanet for a city-of-watauga meter.
    This method is intended to run as a Batch (not celery) task. Pass in SQLAlchemy
    objects and the batch job id.
    """
    urjanet_ingest_base(
        account,
        meter,
        datasource,
        params,
        urjanet_datasource.WataugaDatasource(meter.utility_account_id),
        urjanet_transformer.WataugaTransformer(),
        task_id)


def southlake_ingest_batch(account: SnapmeterAccount, meter: Meter,
                         datasource: MeterDataSource, params: dict,
                         task_id: Optional[str] = None):
    """
    Get data from Urjanet for a city-of-southlake meter.
    """
    urjanet_ingest_base(
        account,
        meter,
        datasource,
        params,
        urjanet_datasource.SouthlakeDatasource(meter.utility_account_id),
        urjanet_transformer.SouthlakeTransformer(),
        task_id)


def mountainview_ingest_batch(account: SnapmeterAccount, meter: Meter,
                         datasource: MeterDataSource, params: dict,
                         task_id: Optional[str] = None):
    """
    Get data from Urjanet for a city-of-mountain-view meter.
    This method is intended to run as a Batch (not celery) task. Pass in SQLAlchemy
    objects and the batch job id.
    """
    urjanet_ingest_base(
        account,
        meter,
        datasource,
        params,
        urjanet_datasource.MountainViewDatasource(meter.utility_account_id),
        urjanet_transformer.GenericWaterTransformer(),
        task_id)


# Look up scraper function according to the Meter Data Source name recorded in the database.
scraper_functions = {
    "watauga-urjanet": watauga_ingest_batch,
    "southlake-urjanet": southlake_ingest_batch,
    "mountainview-urjanet": mountainview_ingest_batch,
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

