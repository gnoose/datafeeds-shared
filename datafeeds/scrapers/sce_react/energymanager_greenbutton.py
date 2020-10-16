import re
import logging
from typing import Optional, Dict, Any

from dateutil.parser import parse as parse_date
import requests
from urllib.parse import urlencode

from datafeeds.common.exceptions import ApiError, DataSourceConfigurationError
from datafeeds.common.util.pagestate.pagestate import PageStateMachine

from datafeeds.common.timeline import Timeline
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status
from datafeeds import config
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
import datafeeds.scrapers.sce_react.pages as sce_pages
from datafeeds.scrapers.sce_react.energymanager_interval import (
    SceReactEnergyManagerIntervalScraper,
)

log = logging.getLogger(__name__)


class SceReactEnergyManagerGreenButtonConfiguration(Configuration):
    """React Energy Manager Scraper configuration

    Current configuration options:
        service_id: The SCE service id to extract data for
        meta: datasource meta
        meter: meter being scraped
    """

    def __init__(self, service_id: str, meta: Dict[str, Any], meter: Meter):
        super().__init__(scrape_bills=False, scrape_readings=True)
        self.service_id = service_id
        self.meta = meta
        self.meter = meter


class SceReactEnergyManagerGreenButtonScraper(SceReactEnergyManagerIntervalScraper):
    def _execute(self):
        self.interval_data_timeline = None
        state_machine = self.define_state_machine()
        final_state = state_machine.run()
        if final_state == "done":
            if self.interval_data_timeline:
                serialized = self.interval_data_timeline.serialize()
                self.log_readings(serialized)
                return Results(readings=serialized)
            return Results(readings={})
        raise Exception(
            "The scraper did not reach a finished state. This will require developer attention."
        )

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
            transitions=["done"],
        )
        # And that's the end
        state_machine.add_state("done")

        state_machine.validate()
        state_machine.set_initial_state("init")
        return state_machine

    def landing_page_action(self, page: sce_pages.SceLandingPage):
        service_account_num = self._configuration.service_id

        service_account_num = service_account_num.replace("-", "")  # remove dashes
        service_account_num = int(service_account_num[1:])  # remove leading digit

        timeline = Timeline(self.start_date, self.end_date)

        # address format is state address city zip
        # try to get from meta
        meta = self._configuration.meta
        if meta and meta.get("serviceAccountAddress"):
            address = meta.get("serviceAccountAddress")
        else:
            # if not available, build it from the meter
            building = self._configuration.meter.building
            if building.street2:
                street_addr = f"{building.street1} {building.street2}"
            else:
                street_addr = building.street1

            city = building.city
            zipcode = building.zip
            address = f"{building.state} {street_addr.upper()} {city.upper()} {zipcode}"

        # day-mon-year
        start_dt = self.start_date.strftime("%d-%m-%Y")
        end_dt = self.end_date.strftime("%d-%m-%Y")

        params = {
            "serviceAccountNumber": service_account_num,
            "serviceAccountAddress": address,
            "startDate": start_dt,
            "endDate": end_dt,
            "fileFormat": "csv",
        }
        url = (
            f"https://prodms.dms.sce.com/myaccount/v1/downloadFile?{urlencode(params)}"
        )
        log.info(f"getting data from {url}")
        cookies = {}
        for cookie in self._driver.get_cookies():
            cookies[cookie["name"]] = cookie["value"]

        # join with ; (ie name1=value1; name2=value2)
        cookie_str = ";".join([k + "=" + v for k, v in cookies.items()])

        oktasessionid = self._driver.execute_script(
            "return JSON.parse(window.localStorage.userInfo).profileData.oktaSessionId;"
        )
        oktauid = self._driver.execute_script(
            "return JSON.parse(window.localStorage.userInfo).profileData.oktaUid;"
        )

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
            "oktasessionid": oktasessionid,  # profileData.oktaSessionId in localstorage
            "oktauid": oktauid,  # profileData.oktaUid in localstorage
            "cookie": cookie_str,
        }

        resp = requests.get(url, headers=headers)
        if resp.status_code >= 400:
            raise ApiError(f"error downloading data: returned {resp.status_code}")
        data = resp.text
        filename = f"{config.WORKING_DIRECTORY}/download.csv"
        with open(filename, "w") as f:
            f.write(data)
        log.debug(f"wrote downloaded data to {filename}")

        # parse out fields starting with date; save start (1st) date and value
        # add to timeline: self.interval_data_timeline.insert(dt, val)
        # regex to match line starting with: "date time
        starts_with_date_re = re.compile(r'^"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')

        lines = data.splitlines()
        log.info(f"downloaded {len(lines)} lines")
        if not lines:
            raise DataSourceConfigurationError(
                "no data downloaded; may need to set serviceAddress metadata"
            )

        to_kw = 60 / self._configuration.meter.interval
        for data_line in lines:
            if not starts_with_date_re.match(data_line):
                continue

            # data_line has a \xa0 space before; remove that
            data_line = data_line.replace("\xa0", " ")
            #  "2020-08-15 00:00:00 to 2020-08-15 00:15:00","2.800",""
            from_dt_string = data_line.split(" to ")[0].replace('"', "")
            from_dt = parse_date(from_dt_string)
            _value = data_line.split('"')[3].replace(
                ",", ""
            )  # not sure if there can be commas in the value but remove them if there are...
            # values are kWh: Usage(Real energy in kilowatt-hours); convert to kW using the meter interval
            value = float(_value) * to_kw
            timeline.insert(from_dt, value)

        self.interval_data_timeline = timeline


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SceReactEnergyManagerGreenButtonConfiguration(
        service_id=meter.service_id, meta=datasource.meta, meter=meter,
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
