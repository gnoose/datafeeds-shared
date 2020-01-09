from typing import Optional
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.datasource import AmericanWaterDatasource
from datafeeds.urjanet.transformer import AmericanTransformer
from datafeeds.common.batch import run_urjanet_datafeed


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
):
    return run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        AmericanWaterDatasource(meter.utility_account_id),
        AmericanTransformer(),
        task_id,
    )
