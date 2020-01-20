"""This scraper collects interval data from the STEM API and formats
it for platform to consume.
"""
from datetime import timedelta
import logging
from typing import Optional

import requests

from datafeeds.config import GROVESTREAMS_API_BASE as API_BASE
from datafeeds.common import Timeline
from datafeeds.common.base import BaseApiScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.battery import TimeSeriesType
from datafeeds.common.exceptions import LoginError, ApiError
from datafeeds.common.support import Configuration as BaseConfiguration, Results
from datafeeds.common.typing import Status
from datafeeds.models import SnapmeterAccount, Meter, SnapmeterMeterDataSource
from datafeeds.parsers.grovestreams import parse_login, parse_intervals
from datafeeds.scrapers.support.time import (
    date_to_datetime,
    dt_to_platform_pst,
    dt_to_epoch_ms,
)


log = logging.getLogger(__name__)


class Configuration(BaseConfiguration):
    def __init__(self, organization_name, component_id, meter_type):
        super().__init__(scrape_readings=True)
        self.organization_name = organization_name
        self.component_id = component_id

        self.meter_type = TimeSeriesType.parse(meter_type)
        lookup = {
            TimeSeriesType.CHARGE: "ChgDischgkWh",
            TimeSeriesType.DISCHARGE: "ChgDischgkWh",
            TimeSeriesType.SYNTHETIC_BUILDING_LOAD: "LoadBeforekWh",
        }

        self.stream_id = lookup[self.meter_type]


class Scraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Grovestreams API Scraper"

    @property
    def organization_name(self):
        return self._configuration.organization_name

    @property
    def component_id(self):
        return self._configuration.component_id

    @property
    def stream_id(self):
        return self._configuration.stream_id

    @property
    def meter_type(self):
        return self._configuration.meter_type

    def _login(self):
        self.sess = requests.Session()

        response = self.sess.post(
            API_BASE + "/login", json=dict(email=self.username, password=self.password)
        )

        if response.status_code != requests.codes.ok:
            raise LoginError("Login failed. Status Code: %s." % response.status_code)

        self.organization_id = parse_login(self.organization_name, response.text)

    def _logout(self):
        """The API doesn't actually have a logout endpoint; this is more of a cleanup method for completeness."""
        self.sess.close()

    def _gather_interval_data(self, start_dt, end_dt):
        start_ms = dt_to_epoch_ms(start_dt)
        end_ms = dt_to_epoch_ms(end_dt)

        params = dict(
            org=self.organization_id,
            startDate=start_ms,
            endDate=end_ms,
            itemsById='[{"compId":"%s","streamId":"%s"}]'
            % (self.component_id, self.stream_id),
        )
        response = self.sess.get(API_BASE + "/feed", params=params)

        if response.status_code != requests.codes.ok:
            raise ApiError(
                "Failed to acquire data from the API. Status Code: %s."
                % response.status_code
            )

        stream_id, intervals = parse_intervals(response.text)
        intervals.sort(key=lambda x: x.start)
        return stream_id, intervals

    def _execute(self):
        log.info("Attempting to log into the Grovestreams API.")
        self._login()
        log.info("Login successful. Organization ID: %s" % self.organization_id)

        timeline = Timeline(self.start_date, self.end_date)

        current_dt = date_to_datetime(self.start_date)
        end_dt = date_to_datetime(self.end_date)
        step = timedelta(days=1)

        while current_dt < end_dt:
            next_dt = current_dt + step
            stream_uid, interval_data = self._gather_interval_data(current_dt, next_dt)

            msg = "Recovered data for stream %s. (UID: %s, Data Points: %s)"
            log.info(msg % (self.stream_id, stream_uid, len(interval_data)))

            for ivl in interval_data:
                pst_time = dt_to_platform_pst(ivl.start)  # Convert UTC to PST
                kw = ivl.kwh * 4  # Convert kWh to kW.

                if self.meter_type == TimeSeriesType.SYNTHETIC_BUILDING_LOAD:
                    timeline.insert(pst_time, kw)
                elif self.meter_type == TimeSeriesType.CHARGE:
                    if kw > 0:
                        timeline.insert(pst_time, kw)
                    else:
                        timeline.insert(pst_time, 0)
                elif self.meter_type == TimeSeriesType.DISCHARGE:
                    if kw < 0:
                        timeline.insert(pst_time, -kw)
                    else:
                        timeline.insert(pst_time, 0)

            current_dt = next_dt

        self._logout()
        return Results(readings=timeline.serialize())


# Grovestreams does not have a notion of an account id / service id the way utilities do.
# Instead, meters have "component ids" and are grouped in to "organizations". To reduce redundant terminology and
# make our admin UIs consistent, we will use:
#
# - "account id" as a synonym for "organization" and
# - "service_id" as a synonym for "component id".
#
#
def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: SnapmeterMeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = Configuration(
        organization_name=meter.utility_account_id,
        component_id=meter.service_id,
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
