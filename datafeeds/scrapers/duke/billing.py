# pylint: disable=too-few-public-methods
""" Duke Billing scraper module """
import logging
from typing import Optional

import datafeeds.scrapers.duke.pages as duke_pages
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration
from datafeeds.common.support import Results
from datafeeds.common.util.pagestate.pagestate import PageStateMachine
from datafeeds.scrapers.duke import errors
from datafeeds.common.typing import adjust_bill_dates
from datafeeds.common.typing import Status
from datafeeds.common.batch import run_datafeed
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

logger = None
log = logging.getLogger(__name__)


class DukeBillingConfiguration(Configuration):
    """ Duke Billing scraper configuration
        account_id is optional.
    """

    def __init__(self, account_id: str):
        super().__init__(scrape_bills=True, scrape_readings=False)
        self.account_id = account_id


class DukeBillingScraper(BaseWebScraper):
    """ Duke Billing scraper """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "Duke Billing"
        self.billing_history = []

    @property
    def account_id(self):
        """ Return the service ID """
        return self._configuration.account_id

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

        # Next, we login. On success, we get transferred to the Duke landing page.
        state_machine.add_state(
            name="login",
            page=duke_pages.DukeLoginPage(self._driver),
            action=self.login_action,
            transitions=["main_landing_page", "login_failed"],
            wait_time=30,
        )

        # We arrive at this state when a login fails. The scraper fails.
        state_machine.add_state(
            name="login_failed",
            page=duke_pages.DukeLoginFailedPage(self._driver),
            action=self.login_failed_action,
            transitions=["main_landing_page"],
        )

        # We get to the landing page, where we need to open the billing page with all the meters
        state_machine.add_state(
            name="main_landing_page",
            page=duke_pages.DukeLandingPage(self._driver),
            action=self.landing_page_action,
            transitions=["accounts_page"],
        )

        state_machine.add_state(
            name="accounts_page",
            page=duke_pages.DukeAccountsPage(self._driver),
            action=self.accounts_page_action,
            transitions=["done"],
        )

        # And that's the end
        state_machine.add_state("done")

        state_machine.validate()
        state_machine.set_initial_state("init")
        return state_machine

    def _execute(self):
        """ Define, run and return the results from running this SM """
        state_machine = self.define_state_machine()
        final_state = state_machine.run()
        if final_state == "done":
            final_bills = adjust_bill_dates(self.billing_history)
            return Results(bills=final_bills)
        raise errors.BillingScraperException(
            "The scraper did not reach a finished state, "
            "this will require developer attention."
        )

    def init_action(self, _):
        """First action in the state machine"""
        self._driver.get("https://www.duke-energy.com/my-account/single-sign-in")

    def login_action(self, page: duke_pages.DukeLoginPage):
        """Process login action """
        page.login(self.username, self.password)

    @staticmethod
    def login_failed_action(page: duke_pages.DukeLoginFailedPage):
        """Throws an exception on failure to login """
        page.raise_on_error()

    @staticmethod
    def landing_page_action(page: duke_pages.DukeLandingPage):
        """Process landing page action """
        page.open_accounts_page()

    def accounts_page_action(self, page: duke_pages.DukeAccountsPage):
        """Process accounts page action """
        if self.start_date and self.end_date:
            if self.start_date > self.end_date:
                err_msg = "The scraper start date must be before the end date (start={}, end={})".format(
                    self.start_date, self.end_date
                )
                raise errors.BillingScraperInvalidDateRangeException(err_msg)

        if self.account_id:
            self.billing_history = page.process_account(
                self.account_id, self.start_date, self.end_date
            )
        else:
            self.billing_history = page.process_all_accounts(
                self.start_date, self.end_date
            )


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    configuration = DukeBillingConfiguration(meter.utility_account_id)

    return run_datafeed(
        DukeBillingScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
        disable_login_on_error=True,
    )
