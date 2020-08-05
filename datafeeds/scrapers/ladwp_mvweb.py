import gzip
import re
from datetime import timedelta, datetime, date
import logging
from io import BytesIO
from typing import Optional

import javaobj
import requests
from dateutil.tz import gettz, tzutc

from datafeeds import config
from datafeeds.common import Timeline
from datafeeds.common.base import BaseApiScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.exceptions import (
    InvalidMeterDataException,
    LoginError,
    DataSourceConfigurationError,
)
from datafeeds.common.support import Configuration as BaseConfiguration, Results
from datafeeds.common.typing import Status
from datafeeds.models import SnapmeterAccount, Meter, SnapmeterMeterDataSource


MAX_DAYS = 7
log = logging.getLogger(__name__)


class Configuration(BaseConfiguration):
    def __init__(self, mvweb_id: str, is_aggregate: bool):
        super().__init__(scrape_readings=True)
        self.mvweb_id = mvweb_id
        self.is_aggregate = is_aggregate


class Scraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "LADWP MVWeb API Scraper"

    @property
    def mvweb_id(self):
        return self._configuration.mvweb_id

    @property
    def is_aggregate(self):
        return self._configuration.is_aggregate

    def _login(self):
        response = requests.post(
            "http://mvweb.ladwp.com/servlet/com.itron.mvweb.common.servlets.Login",
            data={
                "user": self.username,
                "password": self.password,
                "width": "1187",
                "height": "677",
            },
        )

        outputpath = config.WORKING_DIRECTORY
        with open("{0}/login_response.txt".format(outputpath), "w") as FILE:
            FILE.write(response.text)

        # On an invalid login the mvweb stuff still returns a 200, but it comes back with an error page
        if re.match(r"^.*<TITLE>MV-WEB Login Failed</TITLE>.*$", response.text):
            raise LoginError()

        api_key = None
        for line in response.text.split("\r\n"):
            if re.match(r"^.*KEY=.*$", line):
                api_key = line.split("=")[1].strip('"')
                break
        if api_key:
            return api_key

        else:
            raise DataSourceConfigurationError(
                "Unable to find KEY value for authentication"
            )

    @staticmethod
    def _response_to_javaobj(response):
        with open("example.dat", "wb") as f:
            f.write(response.content)

        buf = BytesIO(response.content)
        gzip_file = gzip.GzipFile(fileobj=buf)
        return javaobj.load(gzip_file)

    def _interval_data(self, session, start, end, timeline: Timeline):
        params = {
            "startDate": start.strftime("%Y%m%d"),
            "endDate": end.strftime("%Y%m%d"),
            "interval": "15",
        }
        if self.is_aggregate:
            url = "http://mvweb.ladwp.com:80/servlet/com.itron.mvweb.usertool.servlets.Fetcher"
            params.update(
                {
                    "request": "AGGREGATORPROFILEDATA",
                    "username": session,
                    "agg_name": self.mvweb_id,
                    "day_start": "0",
                    "unit_type": 1,
                }
            )
        else:
            url = "http://mvweb.ladwp.com:80/servlet/com.itron.mvweb.usertool.servlets.Fetcher"
            params.update(
                {
                    "request": "GetProfileData",
                    "user": self.username,
                    "key": session,
                    "meter": self.mvweb_id,
                    "actualInterval": "15",
                    "gmtOffset": "-480",
                    "dayStart": "0",
                    "channel": "1",
                    "meter_desc": "",
                }
            )

        log.info("requesting data from %s", url)
        response = requests.get(url, params=params)
        log.debug("Response Status: %s", response.status_code)
        if not len(response.content):
            raise InvalidMeterDataException("No data received from %s" % url)
        obj = self._response_to_javaobj(response)

        ptz = gettz("America/Los_Angeles")
        utc = tzutc()

        for intervalobj in obj.myData:
            assert (
                intervalobj.myGmtOffset == 0
            )  # (we convert to ptz so we can ignore myDstOffset)
            if intervalobj.myInterval == -1:  # no data
                continue
            assert intervalobj.myInterval == 15
            start_time = intervalobj.myStartDateTime  # UTC
            current_time = datetime(
                start_time.myYear,
                start_time.myMonth,
                start_time.myDay,
                start_time.myHour,
                start_time.myMinute,
            ).replace(tzinfo=utc)
            for value in intervalobj.myValues:
                timeline.insert(current_time.astimezone(ptz), value * 4)
                current_time += timedelta(minutes=15)

        return timeline

    def _execute(self):
        mvweb_session = self._login()
        # shorten date range reduce load
        if (self.end_date - self.start_date).days > MAX_DAYS:
            self.start_date = self.end_date - timedelta(days=MAX_DAYS)
            log.info(
                "max %s days for MVWeb scraper; adjusting date range to %s - %s",
                MAX_DAYS,
                self.start_date,
                self.end_date,
            )
        current_start = self.start_date
        timeline = Timeline(self.start_date, self.end_date)
        while current_start < self.end_date:
            current_end = min(current_start + timedelta(days=365), self.end_date)
            log.debug("getting data for {}-{}".format(current_start, current_end))
            self._interval_data(mvweb_session, current_start, current_end, timeline)
            current_start = current_end + timedelta(days=1)
        return Results(readings=timeline.serialize())


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: SnapmeterMeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    meta = datasource.meta or {}
    configuration = Configuration(
        mvweb_id=meta.get("mvWebId"),
        is_aggregate="t" in (meta.get("mvWebAggregate", "false") or "false"),
    )
    # reduce load on MVWeb servers: skip if meter has data from within the last 3 days
    max_reading = meter.readings_range.max_date or date.today() - timedelta(days=365)
    interval_age = (date.today() - max_reading).days
    if interval_age <= 3:
        log.info(
            "skipping MVWeb run: meter %s has recent interval data (%s)",
            meter.oid,
            max_reading,
        )
        return Status.SKIPPED
    return run_datafeed(
        Scraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
