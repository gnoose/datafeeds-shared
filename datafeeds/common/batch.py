from datetime import datetime, date, timedelta
import functools as ft
import logging
from typing import Optional, List

from dateutil import parser as dateparser

from datafeeds.common.typing import Status
from datafeeds import db, config
from datafeeds.common import alert, index
from datafeeds.common.exceptions import DataSourceConfigurationError, LoginError
from datafeeds.common.support import Credentials, DateRange
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.transformer.base import UrjanetGridiumTransformer
from datafeeds.urjanet.scraper import BaseUrjanetScraper, BaseUrjanetConfiguration
from datafeeds.models import (
    Meter,
    SnapmeterAccount,
    SnapmeterMeterDataSource as MeterDataSource,
    SnapmeterAccountDataSource as AccountDataSource,
)
from datafeeds.common.upload import upload_bills, upload_readings, attach_bill_pdfs
from datafeeds.common.interval_transform import Transforms


log = logging.getLogger("datafeeds")


def iso_to_dates(start_iso, end_iso):
    if isinstance(start_iso, date):
        start = start_iso
    elif start_iso:
        start = dateparser.parse(start_iso).date()
    else:
        start = date.today() - timedelta(days=365 * 2)

    if isinstance(end_iso, date):
        end = end_iso
    elif end_iso:
        end = dateparser.parse(end_iso).date()
    else:
        end = date.today() - timedelta(days=1)

    if start and end <= start:
        end = start + timedelta(days=1)

    return start, end


def run_datafeed(
    scraper_class,
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    configuration=None,
    task_id=None,
    transforms: Optional[List[Transforms]] = None,
    disable_login_on_error: Optional[bool] = False,
) -> Status:
    transforms = [] if transforms is None else transforms
    acct_hex_id = account.hex_id if account else ""
    acct_name = account.name if account else ""

    bill_handler = ft.partial(upload_bills, meter.utility_service.service_id, task_id)
    readings_handler = ft.partial(
        upload_readings, transforms, task_id, meter.oid, acct_hex_id, datasource.name
    )
    pdfs_handler = ft.partial(attach_bill_pdfs, task_id)
    date_range = DateRange(
        *iso_to_dates(params.get("data_start"), params.get("data_end"))
    )

    parent: AccountDataSource = None
    if datasource.account_data_source:
        parent = datasource.account_data_source
        credentials = Credentials(parent.username, parent.password)
        if not datasource.account_data_source.enabled:
            raise DataSourceConfigurationError(
                "%s scraper for %s is disabled"
                % (datasource.account_data_source.name, meter.oid)
            )
    else:
        credentials = Credentials(None, None)

    if task_id and config.enabled("ES_INDEX_JOBS"):
        log.info("Uploading task information to Elasticsearch.")
        index.index_etl_run(
            task_id,
            {
                "started": datetime.now(),
                "status": "STARTED",
                "accountId": acct_hex_id,
                "accountName": acct_name,
                "meterId": meter.oid,
                "meterName": meter.name,
                "scraper": datasource.name,
                "origin": "datafeeds",
            },
        )
    try:
        error = None
        with scraper_class(credentials, date_range, configuration) as scraper:
            scraper.scrape(
                readings_handler=readings_handler,
                bills_handler=bill_handler,
                pdfs_handler=pdfs_handler,
            )
            status = "SUCCESS"
            retval = Status.SUCCEEDED

    except Exception as exc:
        log.exception("Scraper run failed.")
        status = "FAILURE"
        retval = Status.FAILED
        error = repr(exc)
        # disable the login if scraping threw a LoginError, caller requested disabling on error,
        # and meter data source has a parent account data source
        if isinstance(exc, LoginError) and disable_login_on_error and parent:
            parent.enabled = False
            db.session.add(parent)
            alert.disable_logins(parent)

    if task_id and config.enabled("ES_INDEX_JOBS"):
        log.info("Uploading final task status to Elasticsearch.")
        index.index_etl_run(task_id, {"status": status, "error": error}, update=True)

    return retval


def run_urjanet_datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    urja_datasource: UrjanetPyMySqlDataSource,
    transformer: UrjanetGridiumTransformer,
    task_id: Optional[str] = None,
) -> Status:
    conn = db.urjanet_connection()

    try:
        urja_datasource.conn = conn
        scraper_config = BaseUrjanetConfiguration(
            urja_datasource=urja_datasource,
            urja_transformer=transformer,
            utility_name=meter.utility_service.utility,
            fetch_attachments=True,
        )

        return run_datafeed(
            BaseUrjanetScraper,
            account,
            meter,
            datasource,
            params,
            configuration=scraper_config,
            task_id=task_id,
        )
    finally:
        conn.close()
