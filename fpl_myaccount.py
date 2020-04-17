from datetime import date
import logging

from typing import Optional, Tuple, List

from datafeeds.common import Timeline
from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)


class FPLMyAccountConfiguration(Configuration):
    def __init__(self, account_number: str):
        super().__init__(scrape_bills=True)
        self.account_number = account_number


class EnergyDashboardPage:
    def __init__(self, driver):
        self.driver = driver

    def heatmap(self):
        """
        - click Energy Data
        - click Demand Heatmap
        """
        pass

    def download_data(self, start_date: date, end_date: date):
        """
        - for each Period Ending date after start date
          - click button on top right of chart, then Download as CSV Spreadsheet
          - downloads demand_heatmap_*.csv (ie demand_heatmap_UNIVERSITY_VENTURE_LTD_DBA_121_ALHAMBRA_TOWER_LLC_5110.csv)
          - parse CSV and add to results (see energymanager_interval.py#221

"Category","Series 1 (y)","Series 1 (value)"
"12:00 am","Fri Mar, 20",151.2
"12:00 am","Sat Mar, 21",147.84
"12:00 am","Sun Mar, 22",142.08
"12:00 am","Mon Mar, 23",146.08
        """
        timeline = Timeline(start_date, end_date)
        return timeline.serialize()


class EnergyManagerPage:
    def __init__(self, driver):
        self.driver = driver

    def energy_dashboard(self) -> EnergyDashboardPage:
        """
        - click View Energy Dashboard
        """
        return EnergyDashboardPage(self.driver)


class AccountSummaryPage:
    def __init__(self, driver):
        self.driver = driver

    def visit_dashboard(self) -> EnergyManagerPage:
        """
        - click Visit Energy Dashboard
        """
        return EnergyManagerPage(self.driver)


class AccountDashboardPage:
    def __init__(self, driver):
        self.driver = driver

    def select_account(self, account_id: str) -> AccountSummaryPage:
        """
        - close popup if needed (//*[@id="emailBillPopup"]/div/a)
        - click .account-number-link with text matching self.account_number
        """
        return AccountSummaryPage(self.driver)


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def login(self, username: str, password: str) -> AccountDashboardPage:
        """
        - go to https://www.fpl.com/my-account/login.html
        - login with self.username, self.password
        """
        self.driver.get("https://www.fpl.com/my-account/login.html")

        return AccountDashboardPage(self.driver)


class FPLMyAccountScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "FPL MyAccount"

    @property
    def account_number(self):
        return self._configuration.account_number

    def _execute(self):

        login_page = LoginPage(self._driver)
        dashboard_page = login_page.login(self.username, self.password)
        log.info("Login successful.")
        self.screenshot("post_login")

        account_page = dashboard_page.select_account(self.account_number)
        log.info("Loaded account page.")
        self.screenshot("account_summary")

        energy_manager_page = account_page.visit_dashboard()
        log.info("Loaded energy manager page.")
        self.screenshot("energy_manager")

        energy_dashboard = energy_manager_page.energy_dashboard()
        log.info("Loaded energy dashboard page.")
        self.screenshot("energy_manager")
        energy_dashboard.heatmap()
        self.screenshot("energy_manager_heatmap")
        results = energy_dashboard.download_data(self.start_date, self.end_date)

        return Results(readings=results)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = FPLMyAccountConfiguration(meter.utility_service.utility_account_id)

    return run_datafeed(
        FPLMyAccountScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
