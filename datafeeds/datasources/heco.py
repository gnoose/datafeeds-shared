from typing import Optional

from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.datasource import HecoDatasource
from datafeeds.urjanet.transformer import HecoTransformer
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
        HecoDatasource(meter.utility_account_id, meter.utility_service.service_id),
        HecoTransformer(),
        task_id,
    )
