"""This scraper collects interval data from the Engerginet API."""
import json
import logging
import requests
from typing import Optional

from datetime import timedelta, date, datetime
from dateutil.parser import parse as parse_date

from datafeeds.common import Timeline
from datafeeds.common.base import BaseApiScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.exceptions import LoginError
from datafeeds.common.support import Configuration as BaseConfiguration, Results
from datafeeds.common.typing import Status
from datafeeds.models import SnapmeterAccount, Meter, SnapmeterMeterDataSource


log = logging.getLogger(__name__)


class Configuration(BaseConfiguration):
    def __init__(self, api_key, metering_point):
        super().__init__(scrape_readings=True)
        self.api_key = api_key
        self.metering_point = metering_point


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
        response = self.session.post(
            "https://api.eloverblik.dk/CustomerApi/api/MeterData/GetTimeSeries/%s/%s/Hour"
            % (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            json={
                "meteringPoints": {
                    "meteringPoint": [self._configuration.metering_point]
                }
            },
        ).json()

        data = response["result"][0]["MyEnergyData_MarketDocument"]["TimeSeries"][0][
            "Period"
        ]

        for period in data:
            date_ = parse_date(period["timeInterval"]["end"]).date()
            for point in period["Point"]:
                hour = parse_date("%s:00" % (int(point["position"]) - 1)).time()

                value = float(point["out_Quantity.quantity"])
                timeline.insert(datetime.combine(date_, hour), value)

    def _execute(self):
        log.info("Attempting to log into the eloverblik API.")
        self._login()
        log.info("Login successful")

        timeline = Timeline(self.start_date, self.end_date)

        # break start - end date range into 14 day chunks
        # get and parse data 14 days at a time
        start_date = self.start_date
        while start_date < self.end_date:
            if start_date + timedelta(days=14) > self.end_date:
                end_date = start_date + timedelta(
                    days=(self.end_date - start_date).days
                )
            else:
                end_date = start_date + timedelta(days=14)

            self._get_data(timeline, start_date, end_date)
            start_date = end_date

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
