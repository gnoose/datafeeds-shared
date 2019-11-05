from typing import Optional
from datafeeds.models import SnapmeterAccount, Meter, SnapmeterMeterDataSource as MeterDataSource
from datafeeds.urjanet.datasource import HecoDatasource
from datafeeds.urjanet.transformer import HecoTransformer
from datafeeds.common.batch import run_urjanet_datafeed


def datafeed(account: SnapmeterAccount, meter: Meter,
             datasource: MeterDataSource, params: dict,
             task_id: Optional[str] = None):
    run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        HecoDatasource(meter.utility_account_id),
        HecoTransformer(),
        task_id)
