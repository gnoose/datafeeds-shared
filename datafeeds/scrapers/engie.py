from datetime import timedelta
import logging
from typing import Optional

import requests

from datafeeds.common.base import BaseApiScraper
from datafeeds.common.support import Results
from datafeeds.config import ENGIE_API_BASE as API_BASE, ENGIE_API_KEY as API_KEY
from datafeeds.common.batch import run_datafeed
from datafeeds.common.battery import TimeSeriesType
from datafeeds.common.exceptions import ApiError
from datafeeds.common.support import Configuration as BaseConfiguration
from datafeeds.common.timeline import Timeline
from datafeeds.common.typing import Status
from datafeeds.models import Meter, SnapmeterAccount, SnapmeterMeterDataSource
from datafeeds.parsers.engie import parse_intervals
from datafeeds.scrapers.support import time


log = logging.getLogger(__name__)


#
# Remark: In other scraper classes extending BaseScraper, we use the terms account_id and meter_id/service_id
# to refer to the IDs to look for on a utility's site.
#
# Engie isn't a utility, but I'd like to keep the same terminology here for consistency.
#
# Engie's API does not appear to have a notion of an "account". Therefore, we will ignore that field.
# Meter ID or Service ID will be taken to be Engie's Site UID for the meter.
#


class Configuration(BaseConfiguration):
    def __init__(self, meter_id, meter_type):
        super().__init__(scrape_readings=True)
        self.site_id = meter_id
        self.meter_type = TimeSeriesType.parse(meter_type)


class Scraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Engie API Scraper"

    def _gather_interval_data(self, start_dt, end_dt):
        start_ms = time.dt_to_epoch_ms(start_dt)
        end_ms = time.dt_to_epoch_ms(end_dt)

        lookup = {
            TimeSeriesType.CHARGE: "virtual.system",
            TimeSeriesType.DISCHARGE: "virtual.system",
            TimeSeriesType.SYNTHETIC_BUILDING_LOAD: "virtual.building",
        }

        site_id = self._configuration.site_id
        endpoint = lookup[self._configuration.meter_type]

        params = dict(siteIds=site_id, endpoints=endpoint, first=start_ms, last=end_ms)

        headers = dict(Authorization="Bearer %s" % API_KEY)

        response = requests.get(
            API_BASE + "/ep15/v2.0.0?", params=params, headers=headers
        )

        if response.status_code != requests.codes.ok:
            raise ApiError(
                "Failed to acquire data from the API. Status Code: %s."
                % response.status_code
            )

        intervals = parse_intervals("%s.%s" % (site_id, endpoint), response.text)
        intervals.sort(key=lambda x: x.start)
        return intervals

    def _execute(self):
        site_id = self._configuration.site_id
        meter_type = self._configuration.meter_type

        timeline = Timeline(self.start_date, self.end_date)

        current_dt = time.date_to_datetime(self.start_date)
        end_dt = time.date_to_datetime(self.end_date)
        step = timedelta(days=1)

        while current_dt < end_dt:
            next_dt = current_dt + step
            interval_data = self._gather_interval_data(current_dt, next_dt)

            log.info(
                "Recovered data for site %s. Total Intervals: %s"
                % (site_id, len(interval_data))
            )

            for ivl in interval_data:
                pst_time = time.dt_to_platform_pst(ivl.start)

                if meter_type == TimeSeriesType.SYNTHETIC_BUILDING_LOAD:
                    # TODO: Unclear if we need to difference with the charge/discharge channel here.
                    timeline.insert(pst_time, ivl.kw)
                elif meter_type == TimeSeriesType.CHARGE:
                    if ivl.kw > 0:
                        timeline.insert(pst_time, ivl.kw)
                    else:
                        timeline.insert(pst_time, 0)
                elif meter_type == TimeSeriesType.DISCHARGE:
                    if ivl.kw < 0:
                        timeline.insert(pst_time, -ivl.kw)
                    else:
                        timeline.insert(pst_time, 0)

            current_dt = next_dt

        return Results(readings=timeline.serialize())


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: SnapmeterMeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = Configuration(
        meter_id=meter.service_id,
        meter_type=(datasource.meta or {}).get("meterType", "synthetic_building_load"),
    )

    return run_datafeed(
        Scraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
