import logging
import collections
import time

from typing import Optional, List

import datafeeds.scrapers.sce_react.pages as sce_pages
import datafeeds.scrapers.sce_react.errors as sce_errors

from datetime import timedelta

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from datafeeds.common.util.pagestate.pagestate import PageStateMachine
from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status, BillingDatum
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.models.utility_service import GENERATION_ONLY

log = logging.getLogger(__name__)


# Simple object for holding combined usage and demand information scraped from the SCE site
MergedBillData = collections.namedtuple(
    "MergedBillData", ["start_date", "end_date", "usage_info", "demand_info"]
)


class SceReactBasicBillingConfiguration(Configuration):
    def __init__(
        self,
        service_id: str,
        gen_service_id: str,
        scrape_bills: bool,
        scrape_partial_bills: bool,
    ):
        super().__init__(
            scrape_bills=scrape_bills,
            scrape_partial_bills=scrape_partial_bills,
            partial_type=GENERATION_ONLY,
            scrape_readings=False,
        )
        self.service_id = service_id
        self.gen_service_id = gen_service_id


class SceReactBasicBillingScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SCE React Basic Billing"
        self.billing_history = []
        self.partial_billing_history = []

    @property
    def service_id(self):
        return self._configuration.service_id

    @property
    def gen_service_id(self):
        return self._configuration.gen_service_id

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

        # Next, we login. On success, we get transferred to the SCE landing page. Note that there are several possible
        # landing pages, depending on the nature of the login (see the subsequent states for more details).
        # Else, we go to an error page.
        state_machine.add_state(
            name="login",
            page=sce_pages.SceLoginPage(self._driver),
            action=self.login_action,
            transitions=[
                "single_account_landing",
                "multi_account_landing",
                "login_failed",
            ],
            wait_time=30,
        )

        # We arrive at this state when a login fails. The scraper fails.
        state_machine.add_state(
            name="login_failed",
            page=sce_pages.SceLoginFailedPage(self._driver),
            action=self.login_failed_action,
            transitions=[],
        )

        # There are two possible landing pages; one shows a single service account, the other shows multiple accounts
        # with a search bar. This first state handles the case where only a single service account is visible. We
        # directly access the billing data for the single SAID, if it matches the desired SAID.
        state_machine.add_state(
            name="single_account_landing",
            page=sce_pages.SceSingleAccountLandingPage(self._driver),
            action=self.single_account_landing_page_action,
            transitions=["view_usage_dialog"],
        )

        # This state captures the other possibility, where multiple service accounts are present. In this case, we
        # perform a search for the desired SAID.
        state_machine.add_state(
            name="multi_account_landing",
            page=sce_pages.SceMultiAccountLandingPage(self._driver),
            action=self.multi_account_landing_page_action,
            transitions=["search_success", "search_failure"],
        )

        # If the search fails, we end up here, and the scraper fails.
        state_machine.add_state(
            name="search_failure",
            page=sce_pages.SceAccountSearchFailure(self._driver),
            action=self.search_failure_action,
            transitions=[],
        )

        # If the search succeeds, we open the billing information for the found service id.
        state_machine.add_state(
            name="search_success",
            page=sce_pages.SceAccountSearchSuccess(self._driver),
            action=self.search_success_action,
            transitions=["view_usage_dialog"],
        )

        if self._configuration.scrape_partial_bills:
            # This state is responsible for gathering billing data for the desired SAID.
            # When scraping partial bills, go to the landing page again after this.
            state_machine.add_state(
                name="view_usage_dialog",
                page=sce_pages.SceServiceAccountDetailModal(self._driver),
                action=self.view_usage_action,
                transitions=["multi_account_homepage"],
            )
            # this state is for navigating back to the multi account landing page
            # ( this is required in order to view the Generation Billing Data )
            state_machine.add_state(
                name="multi_account_homepage",
                page=sce_pages.SceMultiAccountLandingPage(self._driver),
                action=self.find_generation_account_action,
                transitions=["find_generation_account_success", "search_failure"],
            )

            # Search for generation SAID
            state_machine.add_state(
                name="multi_account_landing_generation",
                page=sce_pages.SceMultiAccountLandingPage(self._driver),
                action=self.multi_account_landing_page_generation_action,
                transitions=["search_success_generation", "search_failure"],
            )

            # If the search succeeds, we open the billing information for the found service id.
            state_machine.add_state(
                name="find_generation_account_success",
                page=sce_pages.SceAccountSearchSuccess(
                    self._driver, self.gen_service_id
                ),
                action=self.search_success_generation_action,
                transitions=["view_generation_cost_dialog"],
            )
            # Get generation costs and combine with data from other bill data.
            state_machine.add_state(
                name="view_generation_cost_dialog",
                page=sce_pages.SceBilledGenerationUsageModal(self._driver),
                action=self.view_generation_usage_action,
                transitions=["done"],
            )
        else:
            # This state is responsible for gathering billing data for the desired SAID.
            state_machine.add_state(
                name="view_usage_dialog",
                page=sce_pages.SceServiceAccountDetailModal(self._driver),
                action=self.view_usage_action,
                transitions=["done"],
            )

        # And that's the end
        state_machine.add_state("done")

        state_machine.validate()
        state_machine.set_initial_state("init")
        return state_machine

    def _execute(self):
        return self.scrape_billing_data()

    def scrape_billing_data(self) -> Results:
        state_machine = self.define_state_machine()
        final_state = state_machine.run()
        if final_state == "done":
            if self.scrape_partial_bills:
                return Results(bills=self.partial_billing_history)
            else:
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

    def single_account_landing_page_action(
        self, page: sce_pages.SceSingleAccountLandingPage
    ):
        sce_pages.detect_and_close_survey(self._driver)
        service_id = page.get_service_account()
        if service_id != self.service_id:
            raise sce_errors.ServiceIdException(
                "No service ID matching '{}' was found.".format(self.service_id)
            )
        page.open_usage_info()

    def find_generation_account_action(
        self, page: sce_pages.SceMultiAccountLandingPage
    ):
        sce_pages.detect_and_close_survey(self._driver)
        page.scroll_for_service_id(self.gen_service_id)
        time.sleep(5)
        WebDriverWait(
            self._driver,
            10,
            EC.invisibility_of_element_located(sce_pages.GenericBusyIndicatorLocator),
        )

    def multi_account_landing_page_action(
        self, page: sce_pages.SceMultiAccountLandingPage
    ):
        sce_pages.detect_and_close_survey(self._driver)
        page.search_by_service_id(self.service_id)
        time.sleep(5)
        WebDriverWait(
            self._driver,
            10,
            EC.invisibility_of_element_located(sce_pages.GenericBusyIndicatorLocator),
        )

    def search_failure_action(self, page: sce_pages.SceAccountSearchFailure):
        raise sce_errors.ServiceIdException(
            "No service ID matching '{}' was found.".format(self.service_id)
        )

    def search_success_action(self, page: sce_pages.SceAccountSearchSuccess):
        page.view_usage_for_search_result()

    def search_success_generation_action(self, page: sce_pages.SceAccountSearchSuccess):
        page.view_billed_generation_charge()

    def view_usage_action(self, page: sce_pages.SceServiceAccountDetailModal):
        page.select_usage_report()

        usage_info = page.get_usage_info(self.start_date, self.end_date)
        demand_info = page.get_demand_info(self.start_date, self.end_date)

        usage_dict = {(info.start_date, info.end_date): info for info in usage_info}
        demand_dict = {(info.start_date, info.end_date): info for info in demand_info}
        usage_dates = set(usage_dict.keys())
        demand_dates = set(demand_dict.keys())

        merged = []
        for date_range in sorted(usage_dates.union(demand_dates)):
            start, end = date_range

            merged.append(
                MergedBillData(
                    start_date=start,
                    end_date=end,
                    usage_info=usage_dict.get(date_range),
                    demand_info=demand_dict.get(date_range),
                )
            )

        billing_objects = []
        for item in merged:
            datum = BillingDatum(
                start=item.start_date,
                end=item.end_date - timedelta(days=1),
                # no separate statement date
                statement=item.end_date - timedelta(days=1),
                cost=item.usage_info.cost,
                used=item.usage_info.usage if item.usage_info else None,
                peak=item.demand_info.demand if item.demand_info else None,
                items=None,
                attachments=None,
                utility_code=None,
            )
            log.debug("created %s", datum)
            billing_objects.append(datum)

        sce_pages.detect_and_close_modal(self._driver)
        log.info("created %s billing objects", len(billing_objects))
        log.debug("billing_objects=%s", billing_objects)
        self.billing_history = billing_objects

        # Not sure if there is a better way to do this but,
        # Go back to home page in case a generation account is specified
        self._driver.get("https://www.sce.com/mysce/myaccount")

    def view_generation_usage_action(
        self, page: sce_pages.SceBilledGenerationUsageModal
    ):
        gen_billing_objects: List[BillingDatum] = []
        gen_values = page.parse_data()
        log.debug("generation values=%s", gen_values)
        for item in self.billing_history:
            # get generation cost for this bill date; try +- 1 day
            for offset in [-1, 0, 1]:
                value = gen_values.get(item.end - timedelta(days=offset))
                if value is not None:
                    break
            if value is None:
                log.debug("no generation data for %s; skipping", item.end)
                continue
            gen_billing_objects.append(
                BillingDatum(
                    start=item.start,
                    end=item.end,
                    statement=item.statement,
                    cost=value,
                    used=item.used,
                    peak=item.peak,
                    items=item.items,
                    attachments=item.attachments,
                    utility_code=item.utility_code,
                )
            )
        log.info("created %s generation billing objects", len(gen_billing_objects))
        log.debug("gen_billing_objects=%s", gen_billing_objects)
        self.partial_billing_history = gen_billing_objects


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SceReactBasicBillingConfiguration(
        service_id=meter.service_id,
        gen_service_id=meter.utility_service.gen_service_id,
        scrape_bills="billing" in datasource.source_types,
        scrape_partial_bills="partial-billing" in datasource.source_types,
    )

    return run_datafeed(
        SceReactBasicBillingScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
