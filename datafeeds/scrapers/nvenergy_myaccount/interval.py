"""
NV Energy My Account Scraper

This scraper makes use of the REST API that NV Energy provides for
their website. We make use of two endpoints, one for login and one for
serving chart data in order to obtain usage information.

It is also possible to obtain account and meter data this way, at
these endpoints:

userAccount/retrieveAccountList
viewusage/getMeterList

However, we already have this data in our DBs, so we don"t utilize
these endpoints.

Some acronyms:
NVE = NV Energy
JWT = JSON Web Token
"""

# Disable warnings on requests.
# pylint: disable=maybe-no-member

from collections import namedtuple
from datetime import datetime, timedelta
import json
import logging
import os

from dateutil.parser import parse as parse_datetime
import requests
from addict import Dict

from datafeeds.common.base import BaseApiScraper
from datafeeds.common.support import Configuration, Results
from datafeeds.common.timeline import Timeline


log = logging.getLogger(__name__)

NVE_CERT_BUNDLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cert_bundle.pem")


logger = None
NVE_API = "https://services.nvenergy.com/api/1.0/cdx/"


UsagePoint = namedtuple("UsagePoint", ["datetime", "kW"])


class NveLoginError(Exception):
    pass


class NveApiError(Exception):
    pass


class NvEnergyMyAccountConfiguration(Configuration):
    def __init__(self, account_id, meter_id):
        super().__init__(scrape_readings=True)
        self.account_id = account_id
        self.meter_id = meter_id


class NvEnergyMyAccountScraper(BaseApiScraper):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "NV Energy My Account"

    @property
    def account_id(self):
        return self._configuration.account_id

    @property
    def meter_id(self):
        return self._configuration.meter_id

    def _execute(self):
        jwt = None
        token_refresh = None

        dt = timedelta(hours=24)
        current = self.start_date

        timeline = Timeline(self.start_date, self.end_date)

        while current <= self.end_date:
            if not token_refresh or datetime.now() - token_refresh > timedelta(minutes=5):
                if not token_refresh:
                    msg = "Attempting login to NV Energy"
                else:
                    msg = "Refreshing JWT token by logging in to NV energy."

                log.info(msg)

                token_refresh = datetime.now()
                jwt = _login(self.username, self.password)
                log.info("NV Energy login succeeded.")

            readings = _fetch_usage_data(jwt, self.account_id, self.meter_id, current)
            log.info("Acquired {} non-null readings for date {}. Account ID: {}, Meter ID: {}".format(
                len(readings), str(current), self.account_id, self.meter_id
            ))

            for upoint in readings:
                timeline.insert(upoint.datetime, upoint.kW)

            current += dt

        return Results(readings=timeline.serialize())


def _parse_hostile_json(text):
    """Try to parse a string that might not quite be JSON

    NV Energy"s API appears to be inserting this string:

         )]}\",\n

    At the beginning of responses. (Not clear if this is a bug or some
    kind of attempt to prevent rogue API calls.)

    If we see this bad prefix, we try to remove it. If that still
    fails, we raise an exception (for later debugging).
    """
    try:
        value = json.loads(text)
        return value
    except ValueError:
        text = text.replace(")]}\",\n", "")
        return json.loads(text)


def _login(username, password):
    """Attempt to obtain a JWT from NVE API's auth endpoint."""
    body = {
        "username": username,
        "password": password,
        "isLoggingInFromMobileApp": False,
        "nvesource": "CUSTOMER WEB ACCESS(CWA)"
    }
    response = requests.post(NVE_API + "auth/retrieveAuthentication", json=body, verify=NVE_CERT_BUNDLE)

    if response.status_code != requests.codes.ok:
        raise NveLoginError("Failed to login to NVE My Energy. status %d" % response.status_code)

    content = Dict(_parse_hostile_json(response.text))

    jwt = content.ResponseBody.user.jwt

    if not jwt or not isinstance(jwt, str) or not jwt.startswith("JWT "):
        msg = "Invalid JWT authorization. Expected string of the form 'JWT ...', found %s"
        raise NveLoginError(msg % str(jwt))

    return jwt


def _fetch_usage_data(jwt, account_id, meter_id, end_date):
    """Returns: A list of UsagePoint tuples by querying the NVE API. """

    # It may not be the case that accountNumber = userAccountNumber
    # for all scraped accounts. However, on an example with multiple
    # meters and different account numbers, this worked OK.
    body = {
        "interval": "15min",  # Can also be "monthly", "weekly", or "day".
        "meterNumber": meter_id,
        "endDate": str(end_date),
        "accountNumber": account_id,
        "isRequestForTile": False,
        "hideDetailInNet": False,
        "billType": "G",  # Not clear we can treat this as a constant.
        "nvesource": "CUSTOMER WEB ACCESS(CWA)",
        "userAccountNumber": account_id
    }

    headers = {
        "Authorization": jwt
    }

    response = requests.post(NVE_API + "viewusage/getChartData", json=body, headers=headers, verify=NVE_CERT_BUNDLE)

    if response.status_code == requests.codes.unauthorized:
        raise NveApiError("JWT failed to authorize data fetch.")

    if response.status_code != requests.codes.ok:
        raise NveApiError("Failed to fetch data. status: %d" % response.status_code)

    content = Dict(_parse_hostile_json(response.text))

    if not content.isSuccess:
        msg = "NVE API call failed. date: %s"
        log.info(msg % end_date)
        return []

    # Sometimes the API sends us "False" to indicate no chart data was available.
    if not content.ResponseBody.chart:
        msg = "NVE API had no interval data for the account at this date. date: %s, API response: %s"
        log.info(msg % (end_date, str(content.ResponseBody.chart)))
        return []
    if isinstance(content.ResponseBody.chart, str):
        if content.ResponseBody.chart == "No data to display":
            msg = "NVE API had no interval data for the account at this date. date: %s"
            log.info(msg % end_date)
        else:
            msg = "Unexpected NVE API message in chart data (date: %s) : %s"
            log.info(msg % (end_date, content.ResponseBody.chart))
        return []

    _assert_type(content, "ResponseBody", dict)
    _assert_type(content.ResponseBody, "chart", dict)
    _assert_type(content.ResponseBody.chart, "dataProvider", list)

    chart_data = content.ResponseBody.chart.dataProvider

    # Chart data looks like the following, as of 2018-04-17:
    #
    # {
    #     "date": "2018-04-15T00:15:00",
    #     "demand": "38.1200",
    #     "averageUsage": null,
    #     "kWh": "9.5300",
    #     "hev": null,
    #     "off": "9.5300",
    #     "mid": null,
    #     "on": null,
    #     "tempBaloonText": null,
    #     "kVARh": "1.8700",
    #     "thermsBalloonText": "<span style="font-size:16px">kWh: 9.5300<br></span>",
    #     "offBalloonText": "<span style=\"font-size:16px\">Off-peak<br>9.5300 kWh</span>",
    #     "calcCost": "0.00"
    # }

    results = []
    for record in chart_data:
        try:
            # Must convert from use to demand, eg:
            # 1 kWh / 15 minutes = 4 kW
            pt = UsagePoint(datetime=parse_datetime(record.date),
                            kW=float(record.kWh) * 4.0)
            results.append(pt)
        except:  # noqa:E722
            continue  # Could not parse that data point, skip

    return results


def _assert_type(record, key, expected_t):
    if not isinstance(record[key], expected_t):
        msg = "Expected %s to have type %s, found %s. (value was %s)"
        raise ValueError(msg % (key,
                                expected_t.__name__,
                                type(record[key]).__name__,
                                str(record[key])))
