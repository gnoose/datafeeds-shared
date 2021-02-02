from dateutil.relativedelta import relativedelta
import logging
from typing import Optional

from dateutil.tz import tzutc
import requests
from requests import codes

from datafeeds import config
from datafeeds.common.base import BaseApiScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.battery import TimeSeriesType
from datafeeds.common.exceptions import ApiError
from datafeeds.common.support import Configuration as BaseConfiguration, Results
from datafeeds.common.timeline import Timeline

from datafeeds.parsers import stem as parser
from datafeeds.scrapers.support.time import date_to_datetime, dt_to_platform_pst

from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)


log = logging.getLogger(__name__)


def _check_utc(dt):
    if dt.tzinfo != tzutc():
        raise ValueError("Stem API only works with datetimes in UTC")


def _isoformat_time(dt):
    # STEM API expects isoformat strings, which are not
    # available with the standard Z prefix from python's
    # datetime package.
    #
    # Unfortunately we have to do some reformatting here.
    # To protect ourselves, we'll add a check that all times are UTC.
    #
    # isoformat example: 2018-07-03T01:00:00+00:00
    # Desired format: 2018-07-03T01:00:00Z

    _check_utc(dt)
    return dt.isoformat().split("+")[0] + "Z"


# Remark: In other scraper classes extending BaseScraper, we
# use the terms account_id and meter_id to refer to the IDs to
# look for on a utility's site. Stem isn't a utility, but I
# would like to keep the same terminology here for
# consistency.
#
# Here is how to convert between our naming convention and
# stem's naming:
#
# account_id = Stem Client UUID.
# meter_id = Stem Site UUID.


class Configuration(BaseConfiguration):
    def __init__(self, account_id, meter_id, meter_type):
        super().__init__(scrape_readings=True)
        self.account_id = account_id
        self.meter_id = meter_id
        self.meter_type = TimeSeriesType.parse(meter_type)


class Session:
    """A translation of the Stem API into function calls. This object
    performs basic validation that API responses meet a schema, and
    imposes some types on the results.
    """

    def __init__(self, api_base, api_key):
        self.api_base = api_base
        self.api_key = api_key
        self.results_per_page = 100

    # STEM API is fairly uniform with respect to paginating data. This
    # private method lets us avoid duplicating code.
    def _accumulate_results(self, url, endpoint_parser):
        accum = []
        page = 1
        while True:
            resp = requests.get(
                url,
                params={"page": page, "per_page": self.results_per_page},
                headers={"Authorization": "APIKEY %s" % self.api_key},
            )
            # It's also possible to limit the number of clients using
            # an optional name parameter, but we don't use this

            if resp.status_code == codes.ok:
                results = endpoint_parser(resp.text)
                accum += results
                if len(results) < self.results_per_page:
                    # No more pages to consider.
                    break
            elif (
                resp.status_code == codes.bad_request
                and "No results found" in resp.text
            ):
                # No more pages to consider
                break
            else:
                # The API isn't working. Abort.
                msg = "Received unexpected API response. status_code: %d text: %s"
                raise ApiError(msg % (resp.status_code, resp.text))
            page += 1

        return accum

    def clients(self):
        url = self.api_base + "/api/v1/clients"
        return self._accumulate_results(url, parser.parse_clients)

    def sites(self, client_id):
        """Retrieve a list of Site nametuples associated with the input
        client_id (UUID)."""
        url = self.api_base + "/api/v1/clients/%s/sites" % client_id
        return self._accumulate_results(url, parser.parse_sites)

    def get_stream(self, site, start, end, stream_type, resolution_sec=900):
        """Retrieve a list of Interval nametuples from a time range.

        site - A Site nametuple specifying the meter.
        start - Start datetime (inclusive)
        end - End datetime (inclusive)

        resolution_sec - Time in seconds for each interval. The
        default (900) specifies 15 minute intervals. Valid values are
        be 900, 1800, or 3600.
        """
        accum = []
        url = self.api_base + site.link + "/streams"
        delta = relativedelta(months=1)

        _check_utc(start)
        _check_utc(end)

        # No point in polling for intervals before the stated start of the feed.
        t0 = max(start, site.start)
        t1 = min(end, t0 + delta)

        while t0 < t1 and t0 < end:
            params = {
                "start_datetime": _isoformat_time(t0),
                "end_datetime": _isoformat_time(t1),
                "resolution": resolution_sec,
                "stream_type": stream_type,
            }

            resp = requests.get(
                url,
                params=params,
                headers={"Authorization": "APIKEY %s" % self.api_key},
            )
            if resp.status_code == codes.ok:
                results = parser.parse_intervals(resp.text, stream_type)
                accum += results
            elif not (
                resp.status_code == codes.bad_request
                and "No data available" in resp.text
            ):
                # The API isn't working, and it's not just because there is no interval data. Abort.
                msg = "Received unexpected API response. status_code: %d text: %s"
                raise ApiError(msg % (resp.status_code, resp.text))

            t0 = t1
            t1 = min(end, t0 + delta)

        return accum


class Scraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "STEM API Scraper"

    @property
    def account_id(self):
        return self._configuration.account_id

    @property
    def meter_self(self):
        return self._configuration.meter_id

    def _open_session(self):
        sess = Session(config.STEM_API_BASE, config.STEM_API_KEY)

        sites = [
            s
            for s in sess.sites(self._configuration.account_id)
            if s.id == self._configuration.meter_id
        ]

        if not sites:
            msg = "Could not find any sites for this site id."
            log.error(msg)
            raise ApiError(msg)

        if len(sites) > 1:
            log.warning(
                "Something may not be right. Found multiple site records for the configured site id."
            )

        site = sites[0]
        return site, sess

    def _compute_synthetic_building_load(self):
        start_time = date_to_datetime(self.start_date)
        end_time = date_to_datetime(self.end_date)

        site, sess = self._open_session()

        intervals = sess.get_stream(site, start_time, end_time, "MONITOR")
        monitor_timeline = Timeline(self.start_date, self.end_date)

        for iv in intervals:
            ivstart = dt_to_platform_pst(iv.start)
            if iv.kw:
                monitor_timeline.insert(ivstart, iv.kw)

        # We want to export the synthetic building load, which removes
        # the demand of charging the battery and adds back the demand
        # that the battery removed from the grid by discharging. We do
        # this by subtracting the converter feed.

        final_timeline = Timeline(self.start_date, self.end_date)
        conv_intervals = sess.get_stream(site, start_time, end_time, "CONVERTER")
        for civ in conv_intervals:
            ivstart = dt_to_platform_pst(civ.start)
            monitor_val = monitor_timeline.lookup(ivstart)

            if civ.kw is not None and monitor_val is not None:
                final_timeline.insert(ivstart, monitor_val - civ.kw)

        return final_timeline

    def _compute_converter_timeseries(self, sign=1):
        start_time = date_to_datetime(self.start_date)
        end_time = date_to_datetime(self.end_date)

        site, sess = self._open_session()

        final_timeline = Timeline(self.start_date, self.end_date)
        conv_intervals = sess.get_stream(site, start_time, end_time, "CONVERTER")
        for civ in conv_intervals:
            ivstart = dt_to_platform_pst(civ.start)
            if civ.kw is None:
                continue
            if civ.kw * sign > 0:
                final_timeline.insert(ivstart, civ.kw * sign)
            else:
                final_timeline.insert(ivstart, 0.0)

        return final_timeline

    def _execute(self):
        mtype = self._configuration.meter_type

        final_timeline = None

        if mtype == TimeSeriesType.SYNTHETIC_BUILDING_LOAD:
            final_timeline = self._compute_synthetic_building_load()
        elif mtype == TimeSeriesType.CHARGE:
            final_timeline = self._compute_converter_timeseries(sign=1)
        elif mtype == TimeSeriesType.DISCHARGE:
            final_timeline = self._compute_converter_timeseries(sign=-1)

        return Results(readings=final_timeline.serialize())


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = Configuration(
        account_id=meter.utility_account_id,
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
