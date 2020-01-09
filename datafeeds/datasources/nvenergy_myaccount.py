from typing import Optional

from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.common.batch import run_datafeed
from datafeeds.scrapers.nvenergy_myaccount import (
    NvEnergyMyAccountScraper,
    NvEnergyMyAccountConfiguration,
)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    configuration = NvEnergyMyAccountConfiguration(
        account_id=meter.utility_account_id, meter_id=meter.service_id
    )
    return run_datafeed(
        NvEnergyMyAccountScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
