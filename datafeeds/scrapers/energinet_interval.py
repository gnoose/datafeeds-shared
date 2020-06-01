"""This scraper collects interval data from the Engerginet API."""
from datetime import timedelta, date
import json
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


class Scraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Energinet Interval API Scraper"
        self.session = requests.Session()

    def _login(self):
        response = self.session.get(
            "https://api.eloverblik.dk/CustomerApi/api/Token",
            headers={"Authorization": "Bearer %s" % self._configuration.api_key},
        )
        if response.status_code != requests.codes.ok:
            raise LoginError("Login failed; status code = %s" % response.status_code)

        data = json.loads(response.text)
        if "result" not in data:
            raise LoginError("Login failed; missing result in response" % response.text)

        self.session.headers.update({"Authorization": "Bearer %s" % data["result"]})

    def _get_data(self, timeline: Timeline, start: date, end: date):
        """Get up to 14 days of data and insert into timeline."""
        data = self.session.post(
            "https://api.eloverblik.dk/CustomerApi/api/MeterData/GetTimeSeries/%s/%s/Hour"
            % (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            json={
                "meteringPoints": {
                    "meteringPoint": [self._configuration.metering_point]
                }
            },
        )

        # parse result into a Timeline: timeline.insert(datetime, value)
        # data = result[0]["MyEnergyData_MarketDocument"]["TimeSeries"][0]["Period"]
        # date from data["timeInterval"]["end"]
        # time from Point[index]["position"] (1 = 00:00, 24 = 23:00)
        # value from Point[index]["out_Quantity.quantity
        """
        {
    "result": [
        {
            "MyEnergyData_MarketDocument": {
                "TimeSeries": [
                    {
                        "MarketEvaluationPoint": {
                            "mRID": {
                                "codingScheme": "A10",
                                "name": "571313113162139726"
                            }
                        },
                        "Period": [
                            {
                                "Point": [
                                    {
                                        "out_Quantity.quality": "A04",
                                        "out_Quantity.quantity": "591.5",
                                        "position": "1"
                                    },
                                    {
                                        "out_Quantity.quality": "A04",
                                        "out_Quantity.quantity": "588.5",
                                        "position": "2"
                                    },
                                    ...
                                ],
                                "resolution": "PT1H",
                                "timeInterval": {
                                    "end": "2020-05-01T22:00:00Z",
                                    "start": "2020-04-30T22:00:00Z"
                                }                                    
        """

    def _execute(self):
        log.info("Attempting to log into the eloverblik API.")
        self._login()
        log.info("Login successful")

        timeline = Timeline(self.start_date, self.end_date)
        # TODO: break start - end date range into 14 day chunks
        # get and parse data 14 days at a time

        return Results(readings=timeline.serialize())


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: SnapmeterMeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = Configuration(
        api_key=datasource.account_data_source.password, metering_point=meter.service_id
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
