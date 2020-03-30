import json
import requests
import logging

from typing import Optional, Dict
from collections import namedtuple
from dateutil.parser import parse as parse_datetime

from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseApiScraper
from datafeeds.common.support import Configuration, Results
from datafeeds.common.timeline import Timeline
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)


API_HOST = "https://ew.buildingiq.com"

UsagePoint = namedtuple("UsagePoint", ["datetime", "kW"])


class LoginError(Exception):
    pass


class ApiError(Exception):
    pass


class PGEEnergyExpertConfiguration(Configuration):
    def __init__(self, item_id):
        super().__init__()
        self.item_id = item_id


class PGEEnergyExpertScraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Portland GE Energy Expert"
        self.browser_name = "Firefox"

    @property
    def account_id(self):
        return self._configuration.account_id

    @property
    def meter_id(self):
        return self._configuration.meter_id

    def _execute(self):
        timeline = Timeline(self.start_date, self.end_date)

        cookies = _login(self.username, self.password)
        readings = _fetch_usage_data(
            cookies, self._configuration.item_id, self.start_date, self.end_date
        )

        for upoint in readings:
            timeline.insert(upoint.datetime, upoint.kW)

        return Results(readings=timeline.serialize())


def _login(username, password):
    """Obtain login cookies from PGEEE"""

    body = {"userName": username, "password": password, "rememberMe": False}

    # It seems to be typical for this request to take ~12 seconds.
    response = requests.post(API_HOST + "/api/account/login", json=body)

    if response.status_code != requests.codes.ok:
        msg = "Failed to login to Portland GE Energy Expert status %d"
        raise LoginError(msg % response.status_code)

    value = Dict(json.loads(response.text))

    if not value.IsAuthenticated:
        msg = "JSON response shows login failed."
        raise LoginError(msg)

    return response.cookies


def _fetch_usage_data(cookies, item_id, start_date, end_date):
    """ Returns: A list of UsagePoint tuples by querying the PGEEE API """

    body = {
        "Start": start_date.strftime("%m/%d/%Y"),
        "End": end_date.strftime("%m/%d/%Y"),
        "ChartInfoRequest": {
            "CompositePointAggregationFunction": "Aggregation_Function_None"
        },
        "ChartSeriesRequest": [
            {
                "SeriesId": "74dd43b5b01eb249de9a71f9d73dfe58",
                "ItemMoniker": "NorthWrite.Building_%s" % item_id,
                "ItemPath": [int(item_id)],
                "UnitsOfMeasureMoniker": "NorthWrite.EngUnit_724992",
                "TimeAxisOffset": "RelativeToTimeAxis_Day_Minus_0",
                "AggregationFunction": "Aggregation_Function_Sum",
                "AggregationInterval": "Aggregation_Interval_None",
            }
        ],
    }

    response = requests.post(
        API_HOST + "/api/Charting/GetMultiItemDataValues", json=body, cookies=cookies
    )

    if response.status_code == requests.codes.unauthorized:
        raise ApiError("Cookies failed to authorize data fetch.")

    if response.status_code != requests.codes.ok:
        raise ApiError("Failed to fetch data. status: %d" % response.status_code)

    content = json.loads(response.text)

    if not isinstance(content, list) or not len(content) == 1:
        actual = type(content).__name__
        raise ApiError("Unexpected data format. type(content): %s" % actual)

    content = Dict(content[0])

    _assert_type(content, "Dates", list)
    _assert_type(content, "Values", list)

    if len(content.Dates) != len(content.Values):
        msg = (
            "Unexpected data format, mismatched dates an values. dates: %d, values: %d"
        )
        raise ApiError(msg % (len(content.Dates), len(content.Values)))

    # Note: We need to convert from use to demand, eg: 1 kWh / 15 minutes = 4 kW
    results = [
        UsagePoint(datetime=parse_datetime(d), kW=float(v) * 4.0)
        for d, v in zip(content.Dates, content.Values)
    ]

    return results


def _assert_type(record, key, expected_t):
    if not isinstance(record[key], expected_t):
        msg = "Expected %s to have type %s, found %s. (value was %s)"
        raise ValueError(
            msg
            % (key, expected_t.__name__, type(record[key]).__name__, str(record[key]))
        )


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = PGEEnergyExpertConfiguration(datasource.meta.get("itemId"))

    return run_datafeed(
        PGEEnergyExpertScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
