from datetime import datetime, date, timedelta
import functools as ft
import logging
from typing import Optional

from dateutil import parser as dateparser

from datafeeds import db, config
from datafeeds.common.support import Credentials, DateRange
from datafeeds.common import index
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.transformer.base import UrjanetGridiumTransformer
from datafeeds.urjanet.scraper import BaseUrjanetScraper, BaseUrjanetConfiguration
from datafeeds.models import Meter, SnapmeterAccount, \
    SnapmeterMeterDataSource as MeterDataSource, \
    SnapmeterAccountDataSource as AccountDataSource
from datafeeds.common.upload import upload_bills, upload_readings

log = logging.getLogger("datafeeds")


def iso_to_dates(start_iso, end_iso):
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


def run_datafeed(scraper_class, account: SnapmeterAccount, meter: Meter,
                 datasource: MeterDataSource, params: dict, configuration=None,
                 task_id=None, transforms=None):
    acct_hex_id = account.hex_id if account else ""
    acct_name = account.name if account else ""

    bill_handler = ft.partial(upload_bills, meter.utility_service.service_id, task_id)
    readings_handler = ft.partial(upload_readings, transforms, task_id,
                                  meter.oid, acct_hex_id, scraper_class.__name__)
    date_range = DateRange(*iso_to_dates(
        params.get("interval_start"),
        params.get("interval_end")
    ))

    if datasource.account_data_source:
        parent: AccountDataSource = datasource.account_data_source
        credentials = Credentials(parent.username, parent.password)
    else:
        credentials = Credentials(None, None)

    if task_id and config.enabled("ES_INDEX_JOBS"):
        index.index_etl_run(task_id, {
            "started": datetime.now(),
            "status": "STARTED",
            "accountId": acct_hex_id,
            "accountName": acct_name,
            "meterId": meter.oid,
            "meterName": meter.name,
            "scraper": scraper_class.__name__,
        })
    try:
        error = None
        with scraper_class(credentials, date_range, configuration) as scraper:
            scraper.scrape(readings_handler=readings_handler, bills_handler=bill_handler)
            status = "SUCCESS"
    except Exception as exc:
        log.exception("Scraper run failed.")
        status = "FAILURE"
        error = repr(exc)

    if task_id and config.enabled("ES_INDEX_JOBS"):
        index.index_etl_run(task_id, {"status": status, "error": error})


def run_urjanet_datafeed(account: SnapmeterAccount, meter: Meter,
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

        run_datafeed(
            BaseUrjanetScraper,
            account,
            meter,
            datasource,
            params,
            configuration=scraper_config,
            task_id=task_id)
    finally:
        conn.close()
