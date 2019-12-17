from typing import Optional

from datafeeds.common import alert
from datafeeds.common.typing import Status
from datafeeds import db
from datafeeds.models import SnapmeterAccount, SnapmeterAccountMeter, Meter
from datafeeds.models import SnapmeterMeterDataSource as MeterDataSource
from datafeeds.common.batch import run_datafeed
from datafeeds.common.exceptions import DataSourceConfigurationError, LoginError
from datafeeds.scrapers import sdge_myaccount as sdge


def datafeed(account: SnapmeterAccount, meter: Meter,
             datasource: MeterDataSource, params: dict, task_id: Optional[str] = None) -> Status:
    """Run scraper for SDGE MyAccount if enabled.

    Retrying a bad login will lock the account. If a login fails, mark all data sources
    for this account as disabled.
    """
    acct_ds = datasource.account_data_source
    acct_meter = db.session.query(SnapmeterAccountMeter).\
        filter_by(meter=meter.oid, account=account.oid).first()
    configuration = sdge.SdgeMyAccountConfiguration(
        acct_meter.utility_account_id, meter.service_id)
    try:
        return run_datafeed(
            sdge.SdgeMyAccountScraper,
            account,
            meter,
            datasource,
            params,
            configuration=configuration,
            task_id=task_id)
    except LoginError as exc:
        acct_ds.enabled = False
        db.session.add(acct_ds)
        alert.disable_logins(acct_ds)
        raise exc
