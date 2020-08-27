import os
import time
import logging
from urllib.parse import urlencode

import requests

import datafeeds.scrapers.sce_react.pages as sce_pages
from datafeeds.scrapers.sce_react.energymanager_interval import SceReactEnergyManagerIntervalScraper

from typing import Optional

from datafeeds.common.util.pagestate.pagestate import PageStateMachine

from datafeeds.common.timeline import Timeline
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Configuration
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)


class SceReactEnergyManagerGreenButtonConfiguration(Configuration):
    """React Energy Manager Scraper configuration

    Current configuration options:
        service_id: The SCE service id to extract data for
        meta: datasource meta
        meter: meter being scraped
    """

    def __init__(self, service_id: str, meta: str, meter: Meter):
        super().__init__(scrape_bills=False, scrape_readings=True)
        self.service_id = service_id
        self.meta = meta
        self.meter = meter


class SceReactEnergyManagerGreenButtonScraper(SceReactEnergyManagerIntervalScraper):

    def define_state_machine(self):
        """Define the flow of this scraper as a state machine"""

        # When we enter a new state, take a screenshot
        def enter_state_callback(state_name):
            self.screenshot("enter_state_{}".format(state_name))

        state_machine = PageStateMachine(self._driver)

        state_machine.on_enter_state(enter_state_callback)

        # We start in the init state, which navigates to the login page
        state_machine.add_state(
            name="init", action=self.init_action, transitions=["login"]
        )

        # Next, we login. On success, we get transferred to the SCE landing page. Else, we go to an error page.
        state_machine.add_state(
            name="login",
            page=sce_pages.SceLoginPage(self._driver),
            action=self.login_action,
            transitions=["landing_page", "login_failed"],
            wait_time=30,
        )

        # We arrive at this state when a login fails
        state_machine.add_state(
            name="login_failed",
            page=sce_pages.SceLoginFailedPage(self._driver),
            action=self.login_failed_action,
            transitions=[],
        )

        # This is the landing page, reached upon successful login. From here generate a URL to download data.
        state_machine.add_state(
            name="landing_page",
            page=sce_pages.SceLandingPage(self._driver),
            action=self.landing_page_action,
            transitions=[],
        )
        # And that's the end
        state_machine.add_state("done")

        state_machine.validate()
        state_machine.set_initial_state("init")
        return state_machine

    def landing_page_action(self, page: sce_pages.SceLandingPage):
        pass
        # TODO:
        service_account_num = self._configuration.service_id
        timeline = Timeline(self.start_date, self.end_date)
        """
        - remove first digit
        - remove -
        - to int
        example: service_id = 3-049-8417-81 to service_account_num = 49841602
        """
        # address format is state address city zip
        # try to get from meta
        meta = self._configuration.meta
        if meta and meta.get("serviceAccountAddress"):
            address = meta.get("serviceAccountAddress")
        else:
            # if not available, build it from the meter
            building = self._configuration.meter.building
            if building.address2:
                street_addr = f"{building.address1} {building.address2}"
            else:
                street_addr = building.street1
            address = f"{building.state} {street_addr.upper()} {city.upper()} {zip}"
        address = urlencode(address)
        # day-mon-year
        start_dt = self.start_date.strftime("%d-%m-%Y")
        end_dt = self.end_date.strftime("%d-%m-%Y")
        url = f"https://prodms.dms.sce.com/myaccount/v1/downloadFile?serviceAccountNumber={service_account_num}" \
              f"&serviceAccountAddress={address}&startDate={start_dt}&endDate={end_dt}&fileFormat=csv"
        log.debug("url = %s", url)
        log.debug("cookies = %s", self._driver.get_cookies())
        # join with ; (ie name1=value1; name2=value2)
        cookie_str = ""
        headers = {
            # these are always the same
            "authority": "prodms.dms.sce.com",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6)",
            "content-type": "application/json",
            "accept": "application/json, text/plain, */*",
            "x-is-secure": "false",
            "origin": "https://www.sce.com",
            "sec-fetch-site": "same-site",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "referer": "https://www.sce.com/sma/ESCAA/EscGreenButtonData",
            "accept-language": "en-US,en;q=0.9",
            # get these from localstorage
            # see https://stackoverflow.com/questions/46361494/how-to-get-the-localstorage-with-python-and-selenium-webdriver
            "oktasessionid": "1",  # profileData.oktaSessionId in localstorage
            "oktauid": "",  # profileData.oktaUid in localstorage
            "cookie": cookie_str
        }
        data = requests.get(url, headers=headers)
        # parse out fields starting with date; save start (1st) date and value
        # add to timeline: self.interval_data_timeline.insert(dt, val)
        """
Energy Usage Information
"For location: CA 3111 N TUSTIN ST ORANGE 92865"

Meter Reading Information
"Type of readings: Electricity"

Summary of Electric Power Usage Information*
"Your download will contain interval usage data that is currently available for your selected Service Account. Based on how our systems process and categorize usage data, your download may contain usage data of the following types: actual, estimated, validated or missing. "

Detailed Usage
"Start date: 2020-08-14 23:00:00  for 11 days"

"Data for period starting: 2020-08-15 00:00:00  for 24 hours"
Energy consumption time period,Usage(Real energy in kilowatt-hours),Reading quality
"2020-08-15 00:00:00 to 2020-08-15 00:15:00","2.800",""
"2020-08-15 00:15:00 to 2020-08-15 00:30:00","2.800",""
   ^^^ date                                  ^^^ value
        """


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SceReactEnergyManagerGreenButtonConfiguration(
        service_id=meter.service_id,
        meta=datasource.meta,
        meter=meter,
    )

    return run_datafeed(
        SceReactEnergyManagerGreenButtonScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
        disable_login_on_error=True,
    )
