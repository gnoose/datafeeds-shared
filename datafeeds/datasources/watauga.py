from typing import Optional

from common.typing import Status
from datafeeds.models import SnapmeterAccount, Meter, SnapmeterMeterDataSource as MeterDataSource
from datafeeds.urjanet.datasource import WataugaDatasource
from datafeeds.urjanet.transformer import WataugaTransformer
from datafeeds.common.batch import run_urjanet_datafeed


def datafeed(account: SnapmeterAccount, meter: Meter,
             datasource: MeterDataSource, params: dict,
             task_id: Optional[str] = None) -> Status:
    return run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        WataugaDatasource(meter.utility_account_id),
        WataugaTransformer(),
        task_id=task_id)
