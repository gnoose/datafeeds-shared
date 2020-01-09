from typing import Optional
from datafeeds.common.typing import Status

from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.scrapers import bloom_interval
from datafeeds.common.batch import run_datafeed


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    configuration = bloom_interval.BloomGridConfiguration(
        site_name=datasource.meta.get("site_name")
    )

    return run_datafeed(
        bloom_interval.BloomScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
