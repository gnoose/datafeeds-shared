from typing import Optional

from datafeeds.common.batch import run_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.scrapers.nvenergy_myaccount import (
    NvEnergyMyAccountConfiguration,
    NvEnergyMyAccountScraper,
)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    configuration = NvEnergyMyAccountConfiguration(
        meter.utility_service.tility_account_id, meter.service_id
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
