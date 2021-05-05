from selenium.webdriver.common.keys import Keys
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.util.pagestate.pagestate import PageStateMachine, PageState
import logging
from typing import Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status
from datafeeds.models import (
    Meter,
    SnapmeterAccount,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.scrapers.smud_first_fuel_interval import (
    SMUDFirstFuelScraper,
    LoginFailedPage,
    UsagePage as SMUDUsagePage,
)

log = logging.getLogger(__name__)


class PepcoIntervalConfiguration(Configuration):
    def __init__(self, utility_account_id: str, service_id: str, interval: int):
        super().__init__(scrape_readings=True)
        self.utility_account_id = utility_account_id
        self.service_id = service_id
        self.interval = interval


class LoginPage(PageState):
    def get_ready_condition(self):
        return EC.presence_of_element_located((By.ID, "Username"))

    def login(self, username, password):
        """Authenticate with the webpage.

        Fill in the username, password, then click "continue"
        """
        self.driver.wait().until(EC.presence_of_element_located((By.ID, "Username")))
        log.info("Inserting credentials on login page.")

        self.driver.find_elements_by_id("Username")[-1].send_keys(username)
        self.driver.find_elements_by_id("Password")[-1].send_keys(password)
        self.driver.find_elements_by_xpath('//button[text()=" Sign In "]')[-1].click()


class ChangeAccountPage(PageState):
    AccountNumberBoxSelector = 'input[type="search"]'

    def get_ready_condition(self):
        return EC.presence_of_element_located((By.CSS_SELECTOR, "#changeAccountDT1"))

    def select_account(self, account_id: str):
        """Find account_id in the Accounts table and click View."""
        account_number_box = self.driver.find_or_raise(self.AccountNumberBoxSelector)
        account_number_box.send_keys(account_id)
        account_number_box.send_keys(Keys.ENTER)
        self.driver.sleep(2)
        self.driver.find_or_raise('//button[.="View"]', xpath=True).click()
        self.driver.sleep(2)


class DashboardPage(PageState):
    def get_ready_condition(self):
        return EC.presence_of_element_located((By.CSS_SELECTOR, "#excNavLeft"))

    def click_green_button(self):
        """Expand My Usage menu, then click Green Button Data."""
        self.driver.sleep(2)
        self.driver.find_or_raise(
            "//div/a[text()='My Usage']/following-sibling::span", xpath=True
        ).click()  # Clicks the expand icon next to "My Usage"
        self.driver.sleep(1)
        self.driver.find("//a[.='My Green Button Data']", xpath=True).click()
        self.driver.screenshot(BaseWebScraper.screenshot_path("select green button"))


class UsagePage(SMUDUsagePage):
    def get_ready_condition(self):
        return EC.presence_of_element_located((By.CSS_SELECTOR, "#FFIframe"))

    def wait_until_iframe_content_ready(self):
        self.driver.wait().until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, self.MeterDropdownSelector)
            ),
        )

    def switch_to_my_usage_iframe(self):
        self.driver.switch_to.frame("FFIframe")

    def switch_back_to_parent_iframe(self):
        self.driver.switch_to.default_content()


class PepcoIntervalScraper(SMUDFirstFuelScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Pepco interval"
        self.timeline = None
        self.url = "https://www.pepco.com"

    def _execute(self):
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
            transitions=["change_account", "dashboard", "login_failed"],
        )

        state_machine.add_state(
            name="login_failed",
            page=LoginFailedPage(self._driver),
            action=self.login_failed_action,
            transitions=[],
        )

        state_machine.add_state(
            name="change_account",
            page=ChangeAccountPage(self._driver),
            action=self.change_account_action,
            transitions=["dashboard"],
        )

        state_machine.add_state(
            name="dashboard",
            page=DashboardPage(self._driver),
            action=self.pepco_dashboard_page_action,
            transitions=["usage"],
            wait_time=30,
        )

        state_machine.add_state(
            name="usage",
            page=UsagePage(self._driver),
            action=self.pepco_usage_page_action,
            transitions=["done"],
        )
        state_machine.add_state("done")

        state_machine.set_initial_state("init")
        final_state = state_machine.run()
        if final_state == "done":
            return Results(readings=self.timeline.serialize() if self.timeline else [])

        raise Exception("The scraper did not reach a finished state.")

    def change_account_action(self, page: ChangeAccountPage):
        """Action for the 'find_account' state. Selects an account based on the account id passed to the scraper."""
        log.info(f"Selecting an account with {self._configuration.utility_account_id}")
        page.select_account(self._configuration.utility_account_id)

    def pepco_dashboard_page_action(self, page: DashboardPage):
        page.click_green_button()

    def pepco_usage_page_action(self, page: UsagePage):

        page.switch_to_my_usage_iframe()
        page.wait_until_iframe_content_ready()

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
    configuration = PepcoIntervalConfiguration(
        utility_account_id=meter.utility_service.utility_account_id,
        service_id=meter.utility_service.service_id,
        interval=meter.interval,
    )

    return run_datafeed(
        PepcoIntervalScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
