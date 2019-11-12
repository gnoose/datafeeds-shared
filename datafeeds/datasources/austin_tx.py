from typing import Optional
from datafeeds.models import SnapmeterAccount, Meter, SnapmeterMeterDataSource as MeterDataSource
from datafeeds.urjanet.datasource import AustinTXDatasource, CommodityType
from datafeeds.urjanet.transformer import AustinTXTransformer
from datafeeds.common.batch import run_urjanet_datafeed


def datafeed(account: SnapmeterAccount, meter: Meter,
             datasource: MeterDataSource, params: dict,
             task_id: Optional[str] = None):
    return run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        AustinTXDatasource(meter.utility_account_id, CommodityType[meter.commodity]),
        AustinTXTransformer(),
        task_id)
