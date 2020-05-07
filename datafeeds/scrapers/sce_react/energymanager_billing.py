import logging
import time
import os

import datafeeds.scrapers.sce_react.pages as sce_pages
import datafeeds.scrapers.sce_react.errors as sce_errors

from typing import Dict, Optional, Tuple

from datetime import date
from datetime import timedelta
from dateutil.relativedelta import relativedelta

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import datafeeds.common.upload as bill_upload

from datafeeds.common.util.pagestate.pagestate import PageStateMachine
from datafeeds.common.util.selenium import ec_or, file_exists_in_dir

from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration, Results, DateRange
from datafeeds.common.typing import Status, BillingDatum
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)


log = logging.getLogger(__name__)


class SceReactEnergyManagerBillingConfiguration(Configuration):
    """React Energy Manager Scraper configuration

    Current configuration options:
        service_id: The SCE service id to extract data for
    """

    def __init__(self, utility: str, utility_account_id: str, service_id: str):
        super().__init__(scrape_bills=True, scrape_readings=False)
        self.service_id = service_id
        self.utility = utility
        self.utility_account_id = utility_account_id


class SceReactEnergyManagerBillingScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SCE React Energy Manager Billing"
        self.billing_history = []

    @property
    def service_id(self) -> str:
        return self._configuration.service_id

    @property
    def utility(self) -> str:
        return self._configuration.utility

    @property
    def utility_account_id(self) -> str:
        return self._configuration.utility_account_id

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
            wait_time=45,
        )

        # We arrive at this state when a login fails
        state_machine.add_state(
            name="login_failed",
            page=sce_pages.SceLoginFailedPage(self._driver),
            action=self.login_failed_action,
            transitions=[],
        )

        # This is the landing page, reached upon successful login. From here we load the energy manager application.
        state_machine.add_state(
            name="landing_page",
            page=sce_pages.SceLandingPage(self._driver),
            action=self.landing_page_action,
            transitions=["energy_manager_landing"],
            wait_time=30,
        )

        # After navigating to Energy Manager, we need to specify the "Service Acct Bills" report type.
        state_machine.add_state(
            name="energy_manager_landing",
            page=sce_pages.SceEnergyManagerLandingPage(self._driver),
            action=self.energy_manager_landing_action,
            transitions=["energy_manager_billing"],
        )

        # Finally, we interact with the "Service Acct Bills" report to dump out some billing data.
        state_machine.add_state(
            name="energy_manager_billing",
            page=sce_pages.SceEnergyManagerBillingPage(self._driver),
            action=self.energy_manager_billing_action,
            transitions=["done"],
        )

        # And that's the end
        state_machine.add_state("done")

        state_machine.validate()
        state_machine.set_initial_state("init")
        return state_machine

    def _execute(self):
        if self.scrape_bills:
            return self.scrape_billing_data()
        log.info("No bill scraping was requested, so nothing to do!")
        return Results(bills=[])

    def scrape_billing_data(self):
        self.billing_history = []
        state_machine = self.define_state_machine()
        final_state = state_machine.run()
        if final_state == "done":
            self.log_bills(self.billing_history)
            return Results(bills=self.billing_history)
        raise Exception(
            "The scraper did not reach a finished state, this will require developer attention."
        )

    def init_action(self, _):
        self._driver.get("https://www.sce.com/mysce/login")

    def login_action(self, page: sce_pages.SceLoginPage):
        page.login(self.username, self.password)

    def login_failed_action(self, page: sce_pages.SceLoginFailedPage):
        # Throw an exception on failure to login
        page.raise_on_error()

    def landing_page_action(self, page: sce_pages.SceLandingPage):
        self._driver.get("https://www.sce.com/mysce/energymanager")

    def energy_manager_landing_action(
        self, page: sce_pages.SceEnergyManagerLandingPage
    ):
        sce_pages.detect_and_close_survey(self._driver)
        time.sleep(5)
        page.select_billing_report()

    def energy_manager_date_range(self, min_start_date):

        if self.start_date:
            start_date = date(
                year=self.start_date.year, month=self.start_date.month, day=1
            )
        else:
            start_date = min_start_date

        if self.end_date:
            end_date = date(year=self.end_date.year, month=self.end_date.month, day=1)
        else:
            today = date.today()
            end_date = date(year=today.year, month=today.month, day=1)

        if start_date > end_date:
            msg = "The scraper start date must be before the end date (start={}, end={})".format(
                start_date, end_date
            )
            sce_errors.BillingDataDateRangeException(msg)
        if end_date < min_start_date:
            msg = "No billing data is available for the range {} to {}.".format(
                start_date, end_date
            )
            raise sce_errors.BillingDataDateRangeException(msg)

        if start_date < min_start_date:
            log.info("Adjusting start date to minimum start date: %s", start_date)
            start_date = min_start_date

        date_range = DateRange(start_date, end_date)
        return date_range

    def energy_manager_billing_action(
        self, page: sce_pages.SceEnergyManagerBillingPage
    ):
        sce_pages.detect_and_close_survey(self._driver)
        page.configure_report()
        page.select_service_id(self.service_id)

        min_start_date = page.get_minimum_selectable_start_date()
        date_range = self.energy_manager_date_range(min_start_date)

        interval_size = relativedelta(months=6)
        raw_billing_data: Dict[Tuple, BillingDatum] = {}
        for subrange in date_range.split_iter(delta=interval_size):
            log.info("Requesting billing data for dates: %s", subrange)
            start = subrange.start_date
            end = subrange.end_date
            page.set_time_range(start, end)

            # Wait for a moment for javascript to stabilize
            time.sleep(5)

            try:
                page.generate_report()
                time.sleep(5)
                WebDriverWait(self._driver, 180).until(
                    EC.invisibility_of_element_located(
                        sce_pages.GenericBusyIndicatorLocator
                    )
                )
            except Exception as e:
                raise sce_errors.EnergyManagerReportException(
                    "Failed to load data from Energy Manager"
                ) from e

            sce_pages.detect_and_close_survey(self._driver)
            try:
                page.raise_on_report_error()
            except sce_errors.EnergyManagerDataNotFoundException:
                # If a given date range has no interval data, just move on to the next one
                continue

            bill_index = 0
            while True:
                visible_bills = page.get_visible_billing_data()
                if bill_index >= len(visible_bills):
                    break
                current_bill_row = visible_bills[bill_index]
                key = (current_bill_row.bill_start_date, current_bill_row.bill_end_date)
                if key not in raw_billing_data:
                    bill_data = BillingDatum(
                        start=current_bill_row.bill_start_date,
                        end=current_bill_row.bill_end_date - timedelta(days=1),
                        statement=current_bill_row.statement_date,
                        cost=current_bill_row.bill_amount,
                        used=current_bill_row.kwh,
                        peak=current_bill_row.kw,
                        items=None,
                        attachments=None,
                    )

                    bill_data = self.download_and_attach_pdf(
                        bill_data, current_bill_row
                    )
                    raw_billing_data[key] = bill_data

                bill_index += 1

        self.billing_history = []
        sorted_ranges = sorted(raw_billing_data.keys())
        for date_range in sorted_ranges:
            self.billing_history.append(raw_billing_data[date_range])

    def download_and_attach_pdf(
        self, bill_data: BillingDatum, billing_row: sce_pages.BillingDataRow
    ) -> BillingDatum:
        self.clear_pdf_downloads()
        bill_path = self.download_pdf_for_billing_row(billing_row)
        if bill_path:
            with open(bill_path, "rb") as bill_file:
                key = bill_upload.hash_bill_datum(self.service_id, bill_data) + ".pdf"
                return bill_data._replace(
                    attachments=[
                        bill_upload.upload_bill_to_s3(
                            bill_file,
                            key,
                            statement=bill_data.statement,
                            source="sce.com",
                            utility=self.utility,
                            utility_account_id=self.utility_account_id,
                        )
                    ]
                )
        else:
            log.info(
                "No pdf bill was available for this period: %s to %s",
                bill_data.start,
                bill_data.end,
            )
            return bill_data

    def download_pdf_for_billing_row(self, billing_row: sce_pages.BillingDataRow):
        error_indicator = "BILL_NOT_AVAILABLE"

        # This is a helper function to help detect when a bill pdf is not available.
        # Somewhat clumsily, it returns a special flag when it detects the error page,
        # which we later check against the return value of the wait clause below.
        def download_error_page_visible(driver):
            locator = (
                By.XPATH,
                "//react-energy-manager//div[contains(@class, 'ServiceAcctBillList__dialogboxError')]",
            )
            query = driver.find_elements(*locator)
            if query:
                return error_indicator
            return None

        billing_row.selector.click()
        time.sleep(2)
        self._driver.find_element_by_id("viewBill").click()
        download_dir = self._driver.download_dir

        # Either the file will download, or SCE will show us an error modal indicating that the bill was
        # not available. If the error page is found, "result" will hold the value in error_indicator,
        # defined above. Else, it will hold the name of the file in the download directory.
        result = WebDriverWait(self._driver, 120).until(
            ec_or(
                download_error_page_visible,
                file_exists_in_dir(download_dir, r".*\.pdf$"),
            )
        )

        if result == error_indicator:
            # We need to make sure to close the modal that appears on error
            close_button_locator = (
                By.XPATH,
                "//react-energy-manager//button[contains(@class, 'sceDialogBox__crossButtonDialogBox')]",
            )
            self._driver.find_element(*close_button_locator).click()
            time.sleep(5)
            return None

        return os.path.join(download_dir, result)

    def clear_pdf_downloads(self):
        """Clean pdf files from the download directory."""
        to_remove = []
        download_dir = self._driver.download_dir
        for filename in os.listdir(download_dir):
            if filename.endswith(".pdf"):
                to_remove.append(os.path.join(download_dir, filename))

        for path in to_remove:
            os.remove(path)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SceReactEnergyManagerBillingConfiguration(
        utility=meter.utility_service.utility,
        utility_account_id=meter.utility_account_id,
        service_id=meter.service_id,
    )

    return run_datafeed(
        SceReactEnergyManagerBillingScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
