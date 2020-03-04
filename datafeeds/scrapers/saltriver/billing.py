from datetime import date, timedelta

from selenium.webdriver.support.ui import WebDriverWait

from typing import Optional
import datafeeds.scrapers.saltriver.pages as saltriver_pages
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration, Results
from datafeeds.common.util.pagestate.pagestate import PageStateMachine, page_is_ready
from datafeeds.common.typing import BillingDatum
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
        billing_start: date,
        billing_end: date,
        account_id: str,
        scrape_bills: bool = True,
    ):
        super().__init__(scrape_bills=scrape_bills, scrape_readings=False)

        self.billing_start = billing_start
        self.billing_end = billing_end
        self.account_id = account_id


class SaltRiverBillingScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SRP Billing Scraper"
        self.billing_history = []

    @property
    def billing_start(self) -> date:
        return self._configuration.billing_start

    @property
    def billing_end(self) -> date:
        return self._configuration.billing_end

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
            transitions=["reports_page"],
        )

        state_machine.add_state(
            name="reports_page",
            page=saltriver_pages.SaltRiverReportsPage(self._driver),
            action=self.reports_page_action,
            transitions=["done"],
        )

        state_machine.add_state("done")

        state_machine.validate()
        state_machine.set_initial_state("init")
        return state_machine

    def _execute(self):
        if self.scrape_bills:
            return self.scrape_billing_data()
        self.log("No bill scraping was requested, so nothing to do!")
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
        self._driver.get("https://spatia.srpnet.com/login/spatialogin.asp")

    def login_action(self, page: saltriver_pages.SaltRiverLoginPage):
        page.login(self.username, self.password)

    def login_failed_action(self, page: saltriver_pages.SaltRiverLoginFailedPage):
        # Throw an exception on failure to login
        page.raise_on_error()

    def landing_page_action(self, _):
        self._driver.get("https://spatia.srpnet.com/itron/features/index.asp")

    def _is_within_bounds(self, bill: BillingDatum):
        return (not self.billing_start or bill.start >= self.billing_start) and (
            not self.billing_end or bill.end <= self.billing_end
        )

    def reports_page_action(self, reports_page: saltriver_pages.SaltRiverReportsPage):
        reports_page.goto_bill_history()
        bill_config_page = saltriver_pages.BillHistoryConfigPage(self._driver)
        WebDriverWait(self._driver, 30).until(page_is_ready(bill_config_page))
        bill_config_page.select_longest_report()
        bill_config_page.select_account(self.account_id)
        bill_config_page.generate_report()

        bill_results_page = saltriver_pages.BillHistoryResultsPage(self._driver)
        WebDriverWait(self._driver, 30).until(page_is_ready(bill_results_page))
        bill_rows = sorted(
            bill_results_page.get_bill_summaries(), key=lambda item: item.stop_date
        )

        bills = []
        for row in bill_rows:
            details = bill_results_page.get_bill_details(row)
            bills.append(
                BillingDatum(
                    start=details.bill_start,
                    end=details.bill_stop - timedelta(days=1),
                    cost=details.cost,
                    used=details.total_kwh,
                    peak=details.on_peak_kw,
                    items=None,
                    attachments=None,
                )
            )

        self.billing_history = [bill for bill in bills if self._is_within_bounds(bill)]


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    update_bills = True
    if "update_bills" in params:
        update_bills = params.get("update_bills")

    # TODO: this billing date information should be handled in the BaseScraper, instead of here
    bill_start = None
    if "bill_after" in params:
        bill_start = params.get("bill_after")
    bill_end = date.today()

    conf = SaltRiverBillingConfiguration(
        account_id=meter.utility_account_id,
        billing_start=bill_start,
        billing_end=bill_end,
        scrape_bills=update_bills,
    )

    return run_datafeed(
        SaltRiverBillingScraper,
        account,
        meter,
        datasource,
        params,
        configuration=conf,
        task_id=task_id,
    )
