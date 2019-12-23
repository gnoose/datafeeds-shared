from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
from math import isnan

import requests
from requests import codes

from datafeeds import config
from datafeeds.common.exceptions import ApiError
from datafeeds.common.base import BaseApiScraper
from datafeeds.common.support import Configuration
from datafeeds.common.support import Results
from datafeeds.parsers import solaredge as parser
from datafeeds.parsers.solaredge import Interval
from datafeeds.scrapers.support.time import date_to_datetime
from datafeeds.common.timeline import Timeline


log = logging.getLogger(__name__)
MAX_INTERVAL_LENGTH = 30
DATE_FORMAT = "%Y-%m-%d"


def _urlencoded_time(dt: datetime) -> str:
    # SolarEdge API expects datetimes to be url-encoded.
    # Specifically, the space character = "%20"
    # isoformat example: 2019-11-01T11:00:00+00:00
    # Desired format: 2019-11-01%2011:00:00
    return dt.isoformat().split('+')[0].replace('T', '%20')


class SolarEdgeConfiguration(Configuration):
    def __init__(self, site_id: str, meter_id: str):
        super().__init__(scrape_readings=True)
        # Used as a query param
        self.site_id = site_id
        self.meter_id = meter_id


class Session:
    """A translation of the SolarEdge API into function calls. This object
    performs basic validation that API responses meet a schema, and
    imposes some types on the results.
    """
    def __init__(self, api_base="https://monitoringapi.solaredge.com", api_key=None):
        self.api_base = api_base
        self.api_key = api_key
        self.format = "application/json"

    # SolarEdge API has maximum of 1 month interval per request
    def _get_results(self, url, endpoint_parser, extra_params: dict = None):
        params = {'api_key': self.api_key, 'format': self.format}
        if extra_params is not None:
            params.update(extra_params)
        # requests url-encodes things that break the API call.
        param_str = "&".join("%s=%s" % (k, v) for k, v in params.items())
        resp = requests.get(url, param_str)
        if resp.status_code == codes.ok:
            results = endpoint_parser(resp.text)
            return results

        else:
            # The API isn't working. Abort.
            msg = "Received unexpected API response. status_code: %d text: %s"
            raise ApiError(msg % (resp.status_code, resp.text))

    def site(self):
        """Retrieve a list of site details associated with the input
        site_id."""
        url = self.api_base + "/details"
        return self._get_results(url, parser.parse_site)

    def get_intervals(self, api_base: str, start, end, installation_date):
        """Construct intervals by dividing the date range into 1 month ranges
        if necessary. Assumes these times are in the site's timezone"""
        accum = []
        url = api_base + "/meters"
        delta = relativedelta(months=1)
        install_date = datetime.strptime(installation_date, DATE_FORMAT)
        t0 = max(datetime(start.year, start.month, start.day), install_date)
        t1 = min(datetime(end.year, end.month, end.day), t0 + delta)

        while t0 < t1 and t0 < datetime(end.year, end.month, end.day):
            start_time = _urlencoded_time(t0)
            end_time = _urlencoded_time(t1)
            required_params = {"timeUnit": "QUARTER_OF_AN_HOUR",
                               "startTime": start_time,
                               "endTime": end_time
                               }
            results = self._get_results(url, parser.parse_intervals, extra_params=required_params)
            for ind, result in enumerate(results):
                results[ind] = Interval(start=start,
                                        kwh=result.kwh,
                                        serial_number=result.serial_number),

            accum += results

            t0 = t1
            t1 = min(datetime(end.year, end.month, end.day), t0 + delta)

        return accum

    @staticmethod
    def meter_readings(ivls: list, meter_id) -> list:
        """Just get data for one meter"""
        new_ivls_list = []
        for k, v in enumerate(ivls):
            if v[0].serial_number != meter_id:
                continue
            new_ivls_list.append(ivls[k])
        return new_ivls_list

    @staticmethod
    def relative_energy(ivls: list) -> list:
        """Meters API returns lifetime energy readings so subtract to get
        each reading. The first reading is None."""
        new_ivls_list = []
        prev = 0
        for k, v in enumerate(ivls):
            new_ivls_list.append(ivls[k])
            if k == 0:
                prev = v[0].kwh
                new_ivl = Interval(start=v[0].start,
                                   kwh=None,
                                   serial_number=v[0].serial_number)
                new_ivls_list[k] = new_ivl
                continue
            if isnan(v[0].kwh):
                continue
            curr = v[0].kwh
            this_reading = curr - prev
            new_ivl = Interval(start=v[0].start,
                               kwh=this_reading,
                               serial_number=v[0].serial_number)
            new_ivls_list[k] = new_ivl
            prev = curr

        return new_ivls_list


class SolarEdgeScraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = 'SolarEdge API Scraper'
        self.site_url = "https://monitoringapi.solaredge.com/site/{}".format(
            self._configuration.site_id
        )
        self.install_date = None
        self.readings = {}
        self.site_tz = None

    @property
    def account_id(self):
        return self._configuration.account_id

    @property
    def meter_self(self):
        return self._configuration.meter_id

    def _open_session(self):
        sess = Session(self.site_url, config.SOLAREDGE_API_KEY)
        site = sess.site()
        self.site_tz = site.time_zone
        self.install_date = site.installation_date
        log.info("Site timezone is %s" % site.time_zone)
        return site, sess

    def _compute_meter_readings(self) -> Timeline:
        start_time_pst = date_to_datetime(self.start_date, 'US/Pacific')
        start_time = start_time_pst.astimezone(self.site_tz)
        end_time = date_to_datetime(self.end_date, 'US/Pacific')
        end_time = end_time.astimezone(self.site_tz)
        site, sess = self._open_session()
        ivls = sess.get_intervals(self.site_url,
                                  start_time,
                                  end_time,
                                  self.install_date)
        meter_ivls = sess.meter_readings(ivls, self.meter_self)
        relative_ivls = sess.relative_energy(meter_ivls)

        # if there are fewer than 10% non-empty intervals for
        # end_date, then drop it
        last_day_count = 0
        for ivl in relative_ivls:
            if ivl.start.day == end_time.day:
                last_day_count += 1
        if last_day_count < 10:
            self.end_date = self.end_date - timedelta(days=1)

        final_timeline = Timeline(self.start_date, self.end_date)
        current_time = start_time_pst
        delta = timedelta(minutes=15)

        for k, iv in enumerate(relative_ivls):
            if iv.kwh is None or isnan(iv.kwh):
                current_time = current_time + delta
                continue
            else:
                #  Multiply by 4 to get the kW reading we store
                kw = iv.kwh * 4
                final_timeline.insert(current_time, kw)
                log.info("Approximate time of reading: %s, kW: %s",
                         current_time, kw)
                current_time = current_time + delta
        return final_timeline

    def _execute(self) -> Results:
        final_timeline = self._compute_meter_readings()
        return Results(readings=final_timeline.serialize())
