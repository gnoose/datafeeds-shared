import logging

from typing import Optional
from datafeeds.common.typing import Status

from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.scrapers import smart_meter_texas as smt
from datafeeds.common.batch import run_datafeed


log = logging.getLogger(__name__)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    """Run the SMT Selenium scraper to gather interval data (<30 days) or request a report asynchronously."""
    esiid = (datasource.meta or {}).get("esiid")

    if esiid is None:
        log.info(
            "Missing ESIID for datasource {}, meter {}.".format(
                datasource.oid, meter.oid
            )
        )

    configuration = smt.SmartMeterTexasConfiguration(esiid)

    return run_datafeed(
        smt.SmartMeterTexasScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
