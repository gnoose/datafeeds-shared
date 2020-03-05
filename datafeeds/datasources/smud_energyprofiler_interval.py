from typing import Optional

from datafeeds.common.typing import Status
from datafeeds.models import SnapmeterAccount, Meter
from datafeeds.models import SnapmeterMeterDataSource as MeterDataSource
from datafeeds.common.batch import run_datafeed
from datafeeds.scrapers import epo_schneider


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    """Check if datasource is enabled; disable on bad login attempts.

    Retrying a bad login will lock the account. If a login fails, mark all data sources
    for this account as disabled.
    """

    configuration = epo_schneider.EnergyProfilerConfiguration(
        base_url="https://smudpm.epo.schneider-electric.com/smudpm/cgi/eponline.exe",
        account_id=meter.utility_account_id,
        epo_meter_id=meter.utility_service.service_id,
    )

    return run_datafeed(
        epo_schneider.EnergyProfilerScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
        disable_login_on_error=True,
    )
