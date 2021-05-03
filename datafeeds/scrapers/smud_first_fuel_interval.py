import csv
import os

from datafeeds.common.exceptions import DataSourceConfigurationError
from datafeeds.common.util.selenium import file_exists_in_dir, scroll_to
from datetime import date
import logging
from typing import Optional

from dateutil.parser import parse as parse_date
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from datafeeds.common.base import BaseWebScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Configuration, Results
from datafeeds.common.timeline import Timeline
from datafeeds.common.typing import Status
from datafeeds.common.util.pagestate.pagestate import PageState, PageStateMachine
from datafeeds.models import (
    Meter,
    SnapmeterAccount,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)


class SMUDFirstFuelConfiguration(Configuration):
    def __init__(self, utility_account_id: str, service_id: str, interval: int):
        super().__init__(scrape_readings=True)
        self.utility_account_id = utility_account_id
        self.service_id = service_id
        self.interval = interval


class LoginPage(PageState):
    UsernameFieldId = "UserId"
    PasswordFieldId = "Password"
    SigninButtonLocator = (By.XPATH, '//button[text()="Sign in"]')

    def get_ready_condition(self):
        return EC.presence_of_element_located((By.ID, self.UsernameFieldId))

    def login(self, username, password):
        """Authenticate with the webpage.

        Fill in the username, password, then click "continue"
        """
        log.info("Inserting credentials on login page.")
        self.driver.find_element_by_id(self.UsernameFieldId).send_keys(username)
        self.driver.find_element_by_id(self.PasswordFieldId).send_keys(password)
        self.driver.find_element(*self.SigninButtonLocator).click()


class LoginFailedPage(PageState):
    LoginErrorLocator = (
        By.XPATH,
        '//div[contains(text(), "Sorry, you could not be authenticated")]',
    )

    def get_ready_condition(self):
        return EC.presence_of_element_located(self.LoginErrorLocator)

    def raise_on_error(self):
        """Raise an exception describing the login error."""
        error = self.driver.find_element(*self.LoginErrorLocator)
        message = "Login failed. The website error is: '{}'".format(error.text)
        raise Exception(message)


class FindAccountPage(PageState):
    def get_ready_condition(self):
        return EC.presence_of_element_located(
            (By.CSS_SELECTOR, "div#account-selection-table")
        )

    def click_account_id(self, account_id):
        """Find account_id in the Accounts table and click it."""
        self.driver.find(f'div[data-account-number="{account_id}"]').click()


class DashboardPage(PageState):
    MyBusinessToolsLinkSelector = "a#link-for-my-business-tools"
    MyEnergyToolkitLinkSelector = "a#link-for-first-engage-entry"

    def get_ready_condition(self):
        return EC.presence_of_element_located(
            (By.CSS_SELECTOR, self.MyBusinessToolsLinkSelector)
        )

    def click_energy_toolkit(self):
        """Click My Business Tools, then My Energy Toolkit."""
        self.driver.sleep(2)
        self.driver.find(self.MyBusinessToolsLinkSelector).click()
        self.driver.sleep(2)
        self.driver.find(self.MyEnergyToolkitLinkSelector).click()


class UsagePage(PageState):
    ExportCSVDropDownButtonLocator = (
        By.XPATH,
        '(//button[contains(@class, "export-button")])[1]',
    )
    FromDateInputSelector = "input#green_button_form_start_green_button_date_range"
    ToDateInputSelector = "input#green_button_form_end_green_button_date_range"
    ExportCSVLinkSelector = "a.track-button-export-green-button-csv-download"
    DownloadButtonSelector = "button.green-button-download"
    MeterDropdownSelector = "button#sdp_selector"

    def get_ready_condition(self):
        return EC.presence_of_element_located(
            (By.CSS_SELECTOR, self.MeterDropdownSelector)
        )

    def export_csv(self, service_id, start: date, end: date) -> str:
        """Export CSV file and return path to downloaded file.

        Select meter service_id from Meter drop down
        Click triple bar button, then Export All Data (CSV)
        Adjust end date if needed: get latest to date from form, parse into a date, and set end to max(end, form_max_dt)
        Set from and to dates (mm/dd/yyyy) and click Download.
        Wait for file to download (.csv)
        Return path to csv file
        """

        self.driver.wait().until(
            EC.invisibility_of_element_located(
                (By.CSS_SELECTOR, "div.spinner-container")
            )
        )

        self.driver.sleep(2)
        self.driver.find(self.MeterDropdownSelector).click()
        # wait for loading
        self.driver.sleep(5)

        meter_dropdown_selector = f'//table[@id="sdp_selector_table"]//a[contains(@class,"sdp-dropdown") and contains(.,"{service_id}")]'
        meter_id_dropdown_option = self.driver.find(meter_dropdown_selector, xpath=True)
        scroll_to(self.driver, meter_id_dropdown_option)
        if not meter_id_dropdown_option:
            raise DataSourceConfigurationError(
                f"No meter found with service_id: {service_id}"
            )

        meter_id_dropdown_option.click()
        self.driver.wait().until(
            EC.invisibility_of_element_located(
                (By.CSS_SELECTOR, "div.spinner-container")
            )
        )
        self.driver.sleep(2)
        self.driver.find_element(*self.ExportCSVDropDownButtonLocator).click()
        self.driver.sleep(2)

        self.driver.find(self.ExportCSVLinkSelector).click()
        self.driver.wait().until(
            EC.visibility_of_element_located(
                (By.CSS_SELECTOR, self.FromDateInputSelector)
            )
        )
        self.driver.sleep(2)
        from_date_input_field = self.driver.find(self.FromDateInputSelector)
        from_date_input_field.clear()
        from_date_input_field.send_keys(start.strftime("%m/%d/%Y"))

        to_date_input_field = self.driver.find(self.ToDateInputSelector)
        max_available_to_date = parse_date(
            to_date_input_field.get_attribute("placeholder")
        ).date()

        end = max(max_available_to_date, end)

        to_date_input_field.clear()
        to_date_input_field.send_keys(end.strftime("%m/%d/%Y"))

        self.driver.find(self.DownloadButtonSelector).click()

        # Wait for csv to download
        download_dir = self.driver.download_dir
        filename = self.driver.wait().until(
            file_exists_in_dir(download_dir, r".*\.{}$".format("csv"))
        )
        return os.path.join(download_dir, filename)


class SMUDFirstFuelScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "SMUD First Fuel"
        self.timeline = None
        self.url = "https://myaccount.smud.org"

    def _parse_csv(self, csv_file_path: str):
        self.timeline = Timeline(self.start_date, self.end_date)
        to_kwh = 60 / self._configuration.interval
        with open(csv_file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    dt = parse_date(row["Start Date Time"].strip())
                    value = row["Usage"].strip()
                    self.timeline.insert(dt, float(value) * to_kwh if value else None)
                except Exception as exc:
                    log.info("skipping row %s: %s", row, exc)

    def _execute(self):
        self._driver.get(self.url)
        log.info(self._configuration.__dict__)

        # We define the scraper flow below using a simple state machine.
        state_machine = PageStateMachine(self._driver)

        state_machine.add_state(
            name="init",
            action=self.init_action,
            transitions=["login"],
        )

        state_machine.add_state(
            name="login",
            page=LoginPage(self._driver),
            action=self.login_action,
            transitions=["find_account", "dashboard", "login_failed"],
        )

        state_machine.add_state(
            name="login_failed",
            page=LoginFailedPage(self._driver),
            action=self.login_failed_action,
            transitions=[],
        )

        state_machine.add_state(
            name="find_account",
            page=FindAccountPage(self._driver),
            action=self.find_account_action,
            transitions=["dashboard"],
        )

        state_machine.add_state(
            name="dashboard",
            page=DashboardPage(self._driver),
            action=self.dashboard_page_action,
            transitions=["usage"],
            wait_time=30,
        )

        state_machine.add_state(
            name="usage",
            page=UsagePage(self._driver),
            action=self.usage_page_action,
            transitions=["done"],
        )
        state_machine.add_state("done")

        state_machine.set_initial_state("init")
        final_state = state_machine.run()
        if final_state == "done":
            return Results(readings=self.timeline.serialize() if self.timeline else [])

        raise Exception("The scraper did not reach a finished state.")

    def init_action(self, _):
        self._driver.get(self.url)

    def login_action(self, page: LoginPage):
        """Action for the 'login' state. Simply logs in with provided credentials."""
        log.info("Attempting to authenticate with the SMUD MyAccount page")
        page.login(self.username, self.password)

    def login_failed_action(self, page: LoginFailedPage):
        """Action for the 'login_failed' state. Gathers errors and raises an exception."""
        page.raise_on_error()

    def find_account_action(self, page: FindAccountPage):
        """Action for the 'find_accoutn' state. Selects an account based on the account id passed to the scraper."""
        log.info(
            "Selecting an account with number: {}".format(
                self._configuration.utility_account_id
            )
        )
        page.click_account_id(self._configuration.utility_account_id)

    def dashboard_page_action(self, page: DashboardPage):
        page.click_energy_toolkit()

    def usage_page_action(self, page: UsagePage):
        filename = page.export_csv(
            self._configuration.service_id, self.start_date, self.end_date
        )
        self._parse_csv(filename)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SMUDFirstFuelConfiguration(
        utility_account_id=meter.utility_service.utility_account_id,
        service_id=meter.utility_service.service_id,
        interval=meter.interval,
    )

    return run_datafeed(
        SMUDFirstFuelScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
