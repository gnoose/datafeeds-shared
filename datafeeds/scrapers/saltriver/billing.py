from typing import Optional
import datafeeds.scrapers.saltriver.pages as saltriver_pages
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration, Results
from datafeeds.common.util.pagestate.pagestate import PageStateMachine
from datafeeds.common.typing import Status
from datafeeds.common.batch import run_datafeed
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)


class SaltRiverBillingConfiguration(Configuration):
    def __init__(
        self,
        account_id: str,
    ):
        super().__init__(scrape_bills=True, scrape_readings=False)

        self.account_id = account_id


class SaltRiverBillingScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SRP Billing Scraper"
        self.billing_history = []

    @property
    def account_id(self) -> str:
        return self._configuration.account_id

    def define_state_machine(self):
        """Define the flow of this scraper as a state machine"""

        # When we enter a new state, take a screenshot
        def enter_state_callback(state_name):
            self.screenshot("enter_state_{}".format(state_name))

        state_machine = PageStateMachine(self._driver)

        state_machine.on_enter_state(enter_state_callback)

        state_machine.add_state(
            name="init", action=self.init_action, transitions=["login"]
        )

        state_machine.add_state(
            name="login",
            page=saltriver_pages.SaltRiverLoginPage(self._driver),
            action=self.login_action,
            transitions=["login_failed", "landing_page"],
            wait_time=45,
        )

        state_machine.add_state(
            name="login_failed",
            page=saltriver_pages.SaltRiverLoginFailedPage(self._driver),
            action=self.login_failed_action,
            transitions=[],
        )

        state_machine.add_state(
            name="landing_page",
            page=saltriver_pages.SaltRiverLandingPage(self._driver),
            action=self.landing_page_action,
            transitions=["done"],
        )

        state_machine.add_state("done")

        state_machine.validate()
        state_machine.set_initial_state("init")
        return state_machine

    def _execute(self):
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
        self._driver.get("https://myaccount.srpnet.com/power/login")

    def login_action(self, page: saltriver_pages.SaltRiverLoginPage):
        page.login(self.username, self.password)

    def login_failed_action(self, page: saltriver_pages.SaltRiverLoginFailedPage):
        # Throw an exception on failure to login
        page.raise_on_error()

    def landing_page_action(self, page: saltriver_pages.SaltRiverLandingPage):
        page.select_account(self.account_id)
        page.set_displayed_bills()
        page.set_history_type()
        self.billing_history = page.get_bills(
            self.account_id, self.start_date, self.end_date
        )


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    conf = SaltRiverBillingConfiguration(account_id=meter.utility_account_id)

    return run_datafeed(
        SaltRiverBillingScraper,
        account,
        meter,
        datasource,
        params,
        configuration=conf,
        task_id=task_id,
    )
