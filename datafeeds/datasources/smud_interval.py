from typing import Optional

from datafeeds import db
from datafeeds.common import alert
from datafeeds.common.typing import Status
from datafeeds.models import SnapmeterAccount, SnapmeterAccountMeter, Meter
from datafeeds.models import SnapmeterMeterDataSource as MeterDataSource
from datafeeds.common.batch import run_datafeed
from datafeeds.common.exceptions import LoginError
from datafeeds.scrapers import epo_schneider


def datafeed(account: SnapmeterAccount, meter: Meter,
             datasource: MeterDataSource, params: dict, task_id: Optional[str] = None) -> Status:
    """Check if datasource is enabled; disable on bad login attempts.

    Retrying a bad login will lock the account. If a login fails, mark all data sources
    for this account as disabled.
    """
    acct_ds = datasource.account_data_source
    acct_meter = db.session.query(SnapmeterAccountMeter).\
        filter_by(meter=meter.oid, account=account.oid).first()
    configuration = epo_schneider.EnergyProfilerConfiguration(
        base_url="https://smudpm.epo.schneider-electric.com/smudpm/cgi/eponline.exe",
        account_id=acct_meter.utility_account_id,
        epo_meter_id=meter.utility_service.service_id)
    try:
        return run_datafeed(
            epo_schneider.EnergyProfilerScraper,
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

