from typing import Optional

from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.datasource import SanDiegoWaterDatasource
from datafeeds.urjanet.transformer import GenericWaterTransformer
from datafeeds.common.batch import run_urjanet_datafeed


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    return run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        SanDiegoWaterDatasource(meter.utility_account_id),
        GenericWaterTransformer(),
        task_id,
    )
