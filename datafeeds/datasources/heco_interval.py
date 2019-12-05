from typing import Optional
from datafeeds.common.typing import Status

from datafeeds.models import SnapmeterAccount, Meter, SnapmeterMeterDataSource as MeterDataSource
from datafeeds.scrapers import heco_interval as he
from datafeeds.common.batch import run_datafeed


def datafeed(account: SnapmeterAccount, meter: Meter,
             datasource: MeterDataSource, params: dict, task_id: Optional[str] = None) -> Status:
    configuration = he.HECOGridConfiguration(meter_id=meter.service_id)

    return run_datafeed(
        he.HECOScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id)
