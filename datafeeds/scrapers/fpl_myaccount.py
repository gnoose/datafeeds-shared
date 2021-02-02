import os
import csv
import logging

from typing import Optional, List
from dateutil.parser import parse as parse_date
from datetime import datetime, date

from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper, CSSSelectorBasePageObject
from datafeeds.common.support import Configuration, Results
from datafeeds.common.util.selenium import file_exists_in_dir
from datafeeds.common.timeline import Timeline
from datafeeds.common.typing import Status, IntervalReading
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


log = logging.getLogger(__name__)


def scroll_to(driver, elem):
    # scroll element into view and scroll up by 82 pixels to account
    # for the navbar's height otherwise the element gets blocked by the navbar
    # (maximum navbar pixel size is 82)
    driver.execute_script("arguments[0].scrollIntoView();scrollBy(0,-82)", elem)


def wait_for_loading_overlay(driver):
    # helper function to wait for page to load
    driver.wait().until(
        EC.invisibility_of_element_located((By.CSS_SELECTOR, "div.window-load"))
    )

    # wait for fade animation
    driver.sleep(0.5)


class FPLMyAccountConfiguration(Configuration):
    def __init__(self, account_number: str):
        super().__init__(scrape_readings=True)
        self.account_number = account_number


class EnergyDashboardPage(CSSSelectorBasePageObject):

    endingperiod_dropdown_sel = "span.ui-selectmenu-button.ui-selectmenu-button-closed"

    def heatmap(self):
        """Click Energy Data, then click Demand Heatmap

        The navbar collapses into a side menu depending on screen size,
        so click the navbar toggle button in case its collapsed
        """
        try:
            self._driver.find_or_raise("span.navbar-toggler-icon").click()
        except Exception:
            pass

        self._driver.sleep(1)
        self._driver.find_or_raise('//a[text()="Energy Data"]', xpath=True).click()

        try:
            self._driver.wait(10).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//a[contains(.,"Demand Heatmap")]')
                )
            ).click()
        except TimeoutException:
            # some accounts don't have a demand heatmap available
            raise Exception("Demand Heatmap not available.")

    def get_available_ending_dates(self) -> List[date]:
        """Get all period ending dates available."""

        available_ending_dates = []

        for list_element in self._driver.find_elements_by_xpath(
            '//ul[contains(@class,"ui-menu")]/li/div'
        ):
            available_ending_dates.append(parse_date(list_element.text).date())

        return available_ending_dates

    def select_period_ending_date(self, ending_date: date):
        """Check if the ending dates dropdown is not already open."""
        if not self._driver.find("span.ui-selectmenu-button.ui-selectmenu-button-open"):
            # open the ending dates dropdown
            self.find_element(self.endingperiod_dropdown_sel).click()

        # xpath for a specific ending_date in the dropdown
        option_selector = f'//ul[contains(@class,"ui-menu")]//div[text()="{ending_date.strftime("%b %d, %Y")}"]'

        log.debug(f'selecting ending date: {ending_date.strftime("%b %d, %Y")}')
        self._driver.find_or_raise(option_selector, xpath=True).click()
        self.wait_for_heatmap_to_load()

    def download_as_csv(self) -> str:
        download_as_csv_sel = '//*[text()="Download as CSV Spreadsheet"]'

        # click the download options button on top right of chart
        self.find_element("g.highcharts-exporting-group").click()

        # click Download as CSV
        self._driver.find_or_raise(download_as_csv_sel, xpath=True).click()

        log.info("Waiting for file to Download")

        try:
            filename = self._driver.wait(30).until(
                file_exists_in_dir(
                    directory=self._driver.download_dir,
                    pattern=r"^demand_heatmap_.+?\.csv$",
                )
            )
        except Exception:
            raise Exception(f"Unable to download file...")

        log.info(f"Download Complete")

        filepath = os.path.join(self._driver.download_dir, filename)

        # rename the file to avoid matching the wrong file in future
        new_filename = f"{date}_demand_heatmap.csv"
        new_filepath = os.path.join(self._driver.download_dir, new_filename)
        os.rename(filepath, new_filepath)

        return new_filepath

    def parse_readings_from_csv(self, csv_file_path: str) -> List[IntervalReading]:
        results = []
        with open(csv_file_path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:

                reading_date = parse_date(row["Series 1 (y)"].strip()).date()
                reading_time = parse_date(row["Category"].strip()).time()

                reading_datetime = datetime.combine(reading_date, reading_time)

                reading_value = row["Series 1 (value)"].strip()

                # some reading values are empty so skip those.
                if not reading_value:
                    continue

                results.append(
                    IntervalReading(dt=reading_datetime, value=float(reading_value))
                )
        return results

    def wait_for_heatmap_to_load(self):
        # wait for heatmap to load
        self._driver.wait().until(
            EC.invisibility_of_element_located(
                (
                    By.XPATH,
                    '//div[@class="highcharts-loading"]/span[text()="Loading..."]',
                )
            )
        )

    def download_data(self, start_date: date, end_date: date):
        """Download data for periods ending after start date.

        For each Period Ending date after start date
          - click button on top right of chart, then Download as CSV Spreadsheet
          - download demand_heatmap_*.csv (ie demand_heatmap_UNIVERSITY_VENTURE_LTD_DBA_121_ALHAMBRA_TOWER_LLC_5110.csv)
          - parse CSV and add to results
        """
        timeline = Timeline(start_date, end_date)

        self.wait_for_heatmap_to_load()

        # we need to open the dates dropdown at least once to get the available dates
        self.find_element(self.endingperiod_dropdown_sel).click()

        for _date in self.get_available_ending_dates():
            # continue if the date isn't in the specified range
            if not (start_date <= _date <= end_date):
                continue

            self.select_period_ending_date(_date)

            log.info(
                f'Downloading heatmap as csv for ending date: {_date.strftime("%b %d, %Y")}'
            )

            csv_file_path = self.download_as_csv()
            readings = self.parse_readings_from_csv(csv_file_path)

            [timeline.insert(reading.dt, reading.value) for reading in readings]

        return timeline.serialize()


class EnergyManagerPage(CSSSelectorBasePageObject):

    view_dashboard_xpath = '//a//div[text()="VIEW ENERGY DASHBOARD"]'

    def view_energy_dashboard(self):
        self._driver.wait().until(
            EC.visibility_of_element_located((By.XPATH, self.view_dashboard_xpath))
        )
        wait_for_loading_overlay(self._driver)

        self._driver.sleep(1)
        # clicking this button right after loading fails,
        # so sleep a little before clicking it
        view_dashboard_btn = self._driver.find_or_raise(
            self.view_dashboard_xpath, xpath=True
        )

        scroll_to(self._driver, view_dashboard_btn)

        self._driver.sleep(0.5)
        view_dashboard_btn.click()

    def _accept_dialog(self):
        # wait until the "Leaving FPL Website" dialog is visible
        self._driver.wait(5).until(
            EC.visibility_of_element_located(
                (
                    By.XPATH,
                    '//div[contains(@class,"modal")]//*[text()="Ready to Save?"]',
                )
            )
        )

        # wait for modal animation
        self._driver.sleep(1)
        self.find_element("div.modal a.ok-button").click()
        # wait for modal animation
        self._driver.sleep(1)

    def energy_dashboard(self) -> EnergyDashboardPage:
        """Click View Energy Dashboard."""

        self.view_energy_dashboard()
        if self._driver.find(
            f'//div[text()="Your User ID or Password is incorrect.  Please try again."]',
            xpath=True,
        ):
            self._driver.refresh()
            self.view_energy_dashboard()

        try:
            self._accept_dialog()

        except TimeoutException:
            # sometimes for unknown reasons the "Leaving FPL Website" dialog
            # fails to open and ._accept_dialog raises TimeoutException,
            # if that's the case try clicking View Energy Dashboard button again
            log.info("the 'Leaving FPL website' dialog didn't appear, trying again...")
            self.view_energy_dashboard()
            self._accept_dialog()

        # it takes a little while for dashboard to open in a new tab, so wait until new tab is open
        _count = 0
        while len(self._driver.window_handles) != 2 or _count < 5:
            self._driver.sleep(0.5)
            _count += 1

        try:
            # dashboard opens in a new tab, switch to the new tab
            dashboard_tab = self._driver.window_handles[1]
            self._driver.switch_to.window(dashboard_tab)
        except IndexError:
            raise Exception("couldn't get energy dashboard tab")

        return EnergyDashboardPage(self._driver)


class AccountSummaryPage(CSSSelectorBasePageObject):

    visit_dashboard_sel = 'a[href="#commercialDashboard"]'

    def visit_dashboard(self) -> EnergyManagerPage:
        """Click Visit Energy Dashboard."""
        self.wait_until_ready(self.visit_dashboard_sel)

        visit_dashboard_elem = self.find_element(self.visit_dashboard_sel)
        scroll_to(self._driver, visit_dashboard_elem)

        log.info("clicking Visit Energy Dashboard")
        visit_dashboard_elem.click()

        wait_for_loading_overlay(self._driver)

        return EnergyManagerPage(self._driver)


class AccountDashboardPage(CSSSelectorBasePageObject):
    def visit_multidashboard(self):
        self._driver.get("https://www.fpl.com/my-account/multi-dashboard.html")
        wait_for_loading_overlay(self._driver)

    def select_account(self, account_id: str) -> AccountSummaryPage:
        """Select an account.

        Close popup if needed (//*[@id="emailBillPopup"]/div/a), then
        click .account-number-link with text matching account_id
        """
        # wait for the accounts list to populate
        self.wait_until_ready(
            "div.accounts-table.multi-card.list div.accounts-list-item"
        )

        account_link_elem = self._driver.find_or_raise(
            f'//a[text()="{account_id}"]', xpath=True
        )

        scroll_to(self._driver, account_link_elem)
        account_link_elem.click()

        wait_for_loading_overlay(self._driver)

        try:
            # there are multiple possible popups that appear on account summary page
            # e.g email, phone no. verification, etc. close any popup if present
            self.wait_until_ready("div.fplModal.modal.fade.in", seconds=10)
            log.info("popup found, closing it...")
            # wait for animation
            self._driver.sleep(1)
            self.find_element(
                "div.fplModal.modal.fade.in a.modal-close.close-x"
            ).click()
            # # wait for animation
            self._driver.sleep(1)
        except TimeoutException:
            pass

        return AccountSummaryPage(self._driver)


class LoginPage(CSSSelectorBasePageObject):
    UsernameFieldSelector = '#loginDiv input[placeholder="Email/User ID"]'
    PasswordFieldSelector = '#loginDiv input[placeholder="Password"]'
    SigninButtonSelector = "#loginDiv .login-page button.standard.btn"

    def login(self, username: str, password: str) -> AccountDashboardPage:
        self._driver.get("https://www.fpl.com/my-account/login.html")

        self.wait_until_ready(self.SigninButtonSelector)
        self._driver.fill(self.UsernameFieldSelector, username)
        self._driver.fill(self.PasswordFieldSelector, password)

        wait_for_loading_overlay(self._driver)
        self.find_element(self.SigninButtonSelector).click()
        wait_for_loading_overlay(self._driver)

        # sometimes after login we're redirected to a different page,
        # if that's the case, manually go to account landing page
        if (
            "https://www.fpl.com/my-account/account-landing.html"
            not in self._driver.current_url
        ):
            log.info(
                f"navigating to: https://www.fpl.com/my-account/account-landing.html"
            )
            self._driver.get("https://www.fpl.com/my-account/account-landing.html")

        # wait for account landing page content to load
        self.wait_until_ready("div#accountLander")
        wait_for_loading_overlay(self._driver)

        return AccountDashboardPage(self._driver)


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

        dashboard_page.visit_multidashboard()
        log.info("Loaded Multi Dashboard Page.")

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
