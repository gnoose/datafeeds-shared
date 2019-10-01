from datetime import datetime, date, timedelta
import logging

import requests
from dateutil import parser as dateparser

from datafeeds import config
from datafeeds.common.support import Credentials, DateRange
from datafeeds.common.typing import BillingData
from datafeeds.common import index, interval_transform, interval_uploader
from datafeeds.models import Meter, SnapmeterAccount, SnapmeterMeterDataSource


log = logging.getLogger(__name__)


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
                 datasource: SnapmeterMeterDataSource, params: dict, configuration=None,
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
        if transforms and readings:
            readings = interval_transform.transform(
                transforms, task_id, scraper_class.__name__, meter.oid, readings)
        interval_uploader.upload_data(readings, account.hex_id, meter.oid)
        if task_id:
            index.update_readings_range(task_id, meter.oid, readings)

    def upload_bills(billing_data: BillingData):
        bills = []
        for bill in billing_data:
            if not bill:
                continue
            bills.append({
                "start": bill.start.strftime("%Y-%m-%d"),
                "end": bill.end.strftime("%Y-%m-%d"),
                "cost": str(bill.cost),
                "used": str(bill.used) if bill.used else "0.0",
                "peak": str(bill.peak) if bill.peak else "0.0",
                "items": bill.items or [],
                "attachments": bill.attachments or []
            })
        response = requests.post(
            "%s/object/utility-service/%s/bills/import" % (config.PLATFORM_API_URL, meter.utility_service.service),
            data={"importance": "product", "bills": bills},
            headers={"Content-type": "application/json", "Accept": "*"}
        )
        response.raise_for_status()
        if task_id:
            index.update_billing_range(task_id, billing_data)

    date_range = DateRange(*scraper_dates(
        params.get("interval_start"),
        params.get("interval_end")
    ))

    credentials = Credentials(
        datasource.get("username", "").strip(),
        datasource.get("password", "").strip(),
    )

    if task_id:
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
        status = "FAILURE"
        error = repr(exc)

    if task_id:
        index.index_etl_run(task_id, {"status": status, "error": error})


if __name__ == '__main__':
    print("Hello world!")
