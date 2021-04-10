import logging
import time
from typing import Optional

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By

from datafeeds.common.exceptions import LoginError
from datafeeds.common.typing import Status
from datafeeds.models import SnapmeterAccount, Meter
from datafeeds.models import SnapmeterMeterDataSource as MeterDataSource
from datafeeds.common.batch import run_datafeed
from datafeeds.scrapers import epo_schneider
from datafeeds.scrapers.epo_schneider import EnergyProfilerScraper

from selenium.webdriver.support import expected_conditions as EC


log = logging.getLogger(__name__)


class LoginPage:
    def __init__(self, driver, login_url: str):
        self._driver = driver
        self.url = login_url

    def goto_page(self):
        self._driver.get(self.url)

    def wait_until_ready(self):
        log.info("Waiting for 'Login' page to be ready...")
        self._driver.wait().until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'input[formcontrolname="username"]')
            )
        )

    def get_continue_button(self):
        return self._driver.find_element_by_css_selector(self.ContinueButtonSelector)

    def login(self, username, password):
        """Authenticate with the webpage.

        Fill in the username, password, then click Sign In
        """
        log.info("Inserting credentials on login page.")
        self._driver.fill('input[formcontrolname="username"]', username)
        self._driver.fill('input[formcontrolname="password"]', password)
        self._driver.find_element_by_css_selector("wcss-epo-login button").click()

        try:
            self._driver.wait(5).until(
                EC.visibility_of_element_located((By.XPATH, "//mat-error"))
            )
            raise LoginError("Invalid username or login.")
        except TimeoutException:
            return  # Login succeeded.


class PacificPowerIntervalScraper(EnergyProfilerScraper):
    def login(self):
        login_page = LoginPage(self._driver, self.base_url)
        # Authenticate
        login_page.goto_page()
        login_page.wait_until_ready()
        self.screenshot("before login")
        try:
            login_page.login(self.username, self.password)
        except LoginError as exc:
            self.screenshot("login failed")
            raise exc
        # go to Energy Profiler
        log.info("clicking Energy Profiler")
        energy_profiler_xpath = "wcss-epo-site button"
        self._driver.wait(30).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, energy_profiler_xpath))
        )
        self._driver.find_element_by_css_selector(energy_profiler_xpath).click()
        time.sleep(5)
        # switch tabs
        log.info("switch to new tab")
        self._driver.switch_to_window(self._driver.window_handles[1])


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    """Check if datasource is enabled; disable on bad login attempts.

    Retrying a bad login will lock the account. If a login fails, mark all data sources
    for this account as disabled.
    """
    meta = datasource.meta or {}
    configuration = epo_schneider.EnergyProfilerConfiguration(
        base_url="https://csapps.pacificpower.net/idm/business-insights",
        account_id=meter.utility_account_id,
        epo_meter_id=meta.get("pacificPowerORMeterNumber"),
        channel_id=meta.get("channelId", None),
    )
    return run_datafeed(
        PacificPowerIntervalScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
        disable_login_on_error=True,
    )
