from typing import Optional
from datafeeds.common.typing import Status

from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.scrapers import solren
from datafeeds.common.batch import run_datafeed


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    configuration = solren.SolrenGridConfiguration(
        inverter_id=meter.service_id, site_id=datasource.meta.get("site_id")
    )

    return run_datafeed(
        solren.SolrenScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
