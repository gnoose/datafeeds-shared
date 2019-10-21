from typing import Optional
from datafeeds.models import SnapmeterAccount, Meter, SnapmeterMeterDataSource as MeterDataSource
from datafeeds.urjanet.datasource import SouthlakeDatasource
from datafeeds.urjanet.transformer import SouthlakeTransformer
from datafeeds.common.batch import run_urjanet_datafeed


def datafeed(account: SnapmeterAccount, meter: Meter,
             datasource: MeterDataSource, params: dict,
             task_id: Optional[str] = None):
    run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        SouthlakeDatasource(meter.utility_account_id),
        SouthlakeTransformer(),
        task_id)
