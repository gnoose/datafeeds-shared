from typing import Optional
import logging

# from datafeeds.common.exceptions import DataSourceConfigurationError
from datafeeds.common.batch import run_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

from datafeeds.scrapers.sce_greenbutton import Configuration, Scraper


log = logging.getLogger(__name__)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
):

    if not ("subscription" in datasource.meta and "usage_point" in datasource.meta):
        msg = (
            "No subscription/usage point pair associated with data source. Skipping. (Data Source OID: %s)"
            % datasource.oid
        )
        log.info(msg)

        # Eventually this will be a genuine failure condition, but until we can completely convert to
        # ingest-based SCE green button, we need to just skip when the data isn't available.
        # raise DataSourceConfigurationError(msg)
        return Status.SKIPPED

    return run_datafeed(
        Scraper,
        account,
        meter,
        datasource,
        params,
        configuration=Configuration(
            subscription=datasource.meta["subscription"],
            usage_point=datasource.meta["usage_point"],
        ),
        task_id=task_id,
    )
