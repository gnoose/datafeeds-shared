import logging
import collections
import time
import uuid

from typing import Optional, List, Dict

from elasticsearch.helpers import bulk
from selenium.common.exceptions import NoSuchElementException

import datafeeds.scrapers.sce_react.pages as sce_pages
import datafeeds.scrapers.sce_react.errors as sce_errors

from datetime import timedelta

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from datafeeds.common.exceptions import InvalidMeterDataException
from datafeeds.common.index import _get_es_connection
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
from datafeeds.scrapers.sce_react.support import detect_and_close_modal

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
        utility_account_id: str,
        scrape_bills: bool,
        scrape_partial_bills: bool,
        metascraper: bool = False,
        account_datasource_id: Optional[int] = None,
    ):
        super().__init__(
            scrape_bills=scrape_bills,
            scrape_partial_bills=scrape_partial_bills,
            scrape_readings=False,
            metascraper=metascraper,
        )
        self.service_id = service_id
        self.gen_service_id = gen_service_id
        self.utility_account_id = utility_account_id
        self.account_datasource_id = account_datasource_id


class SceReactBasicBillingScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "SCE React Basic Billing"
        self.billing_history = []
        self.gen_billing_history = []
        self.utility_tariff_code = None

    @property
    def service_id(self):
        return self._configuration.service_id

    @property
    def gen_service_id(self):
        return self._configuration.gen_service_id

    @property
    def utility_account_id(self):
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
                transitions=[
                    "find_generation_account_success",
                    "find_generation_account_fail",
                ],
            )

            # If the search fails, we're done: nothing to download, and we may already have bills
            # that we want to persist.
            state_machine.add_state(
                name="find_generation_account_fail",
                page=sce_pages.SceGenerationAccountSearchFail(
                    self._driver, self.gen_service_id
                ),
                action=self.search_fail_generation_action,
                transitions=["done"],
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
                # T&D bills use the same pages/path as bundled bills
                return Results(
                    generation_bills=self.gen_billing_history,
                    tnd_bills=self.billing_history,
                )
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

    def log_single_account_ids(self, service_id: str):
        """Create an Elasticsearch record mapping account data source to new service_id."""
        doc = {
            "account_data_source": self._configuration.account_datasource_id,
            "service_id": service_id,
        }
        # TODO: can we get utility account id?
        _get_es_connection().index(
            index="sce-utility-service", id=uuid.uuid4().hex, body=doc
        )

    def single_account_landing_page_action(
        self, page: sce_pages.SceSingleAccountLandingPage
    ):
        sce_pages.detect_and_close_survey(self._driver)
        service_id = page.get_service_account()
        self.log_single_account_ids(service_id)
        if service_id != self.service_id:
            raise sce_errors.ServiceIdException(
                "No service ID matching '{}' was found.".format(self.service_id)
            )
        page.open_usage_info()

    def find_generation_account_action(
        self, page: sce_pages.SceMultiAccountLandingPage
    ) -> bool:
        sce_pages.detect_and_close_survey(self._driver)
        if not page.scroll_for_service_id(self.gen_service_id):
            # if the generation service id can't be found, don't fail the entire scraper

            return False
        time.sleep(5)
        WebDriverWait(
            self._driver,
            10,
            EC.invisibility_of_element_located(sce_pages.GenericBusyIndicatorLocator),
        )
        return True

    def log_multi_account_ids(self, page: sce_pages.SceMultiAccountLandingPage):
        """Get all new utility account ids and service ids and log to Elasticsearch."""
        docs = page.find_address_ids()
        try:
            while page.next_page():
                docs += page.find_address_ids()
        except Exception as exc:
            log.warning(f"error getting address ids: {exc}")
        log.info(
            f"found {len(docs)} address / id docs for account_datasource {self._configuration.account_datasource_id}"
        )
        bulk_docs: List[Dict] = []
        for doc in docs:
            doc["account_data_source"] = self._configuration.account_datasource_id
            bulk_docs.append(
                {
                    "_index": "sce-utility-service",
                    "_type": "_doc",
                    "_id": uuid.uuid4().hex,
                    "_source": doc,
                }
            )
        bulk(_get_es_connection(), bulk_docs)
        log.info(f"indexed {len(docs)} address / id docs")

    def multi_account_landing_page_action(
        self, page: sce_pages.SceMultiAccountLandingPage
    ):
        sce_pages.detect_and_close_survey(self._driver)
        self.utility_tariff_code = page.update_utility_service(self.utility_service)
        page.search_account(self.service_id, self.utility_account_id)

    def search_failure_action(self, page: sce_pages.SceAccountSearchFailure):
        raise sce_errors.ServiceIdException(
            "No service ID matching '{}' was found.".format(self.service_id)
        )

    def search_success_action(self, page: sce_pages.SceAccountSearchSuccess):
        if not page.view_usage_for_search_result(self.service_id):
            raise InvalidMeterDataException(
                "service_id |%s| not found" % self.service_id
            )

    def search_fail_generation_action(self, page: sce_pages.SceAccountSearchFailure):
        log.warning("generation service id not found: |%s|", self.gen_service_id)

    def search_success_generation_action(self, page: sce_pages.SceAccountSearchSuccess):
        page.view_billed_generation_charge()

    def view_usage_action(self, page: sce_pages.SceServiceAccountDetailModal):
        """Scrape data for bundled or T&D bills."""
        page.select_usage_report()

        usage_info = page.get_usage_info(self.start_date, self.end_date)
        try:
            demand_info = page.get_demand_info(self.start_date, self.end_date)
        except NoSuchElementException:
            log.warning("demand report not available")
            demand_info = []

        usage_dict = {(info.start_date, info.end_date): info for info in usage_info}
        demand_dict = {(info.start_date, info.end_date): info for info in demand_info}
        usage_dates = set(usage_dict.keys())
        demand_dates = set(demand_dict.keys())
        log.debug("usage_dict=%s", usage_dict)
        log.debug("demand_dict=%s", demand_dict)
        third_party_expected = self.gen_service_id is not None
        log.debug("date ranges=%s", sorted(usage_dates.union(demand_dates)))

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
            # sometimes cost is not available
            if item.usage_info is None:
                log.warning(
                    "cost not found for %s - %s; skipping",
                    item.start_date,
                    item.end_date,
                )
                continue
            datum = BillingDatum(
                start=item.start_date,
                end=item.end_date,
                # no separate statement date
                statement=item.end_date,
                cost=item.usage_info.cost,
                used=item.usage_info.usage if item.usage_info else None,
                peak=item.demand_info.demand if item.demand_info else None,
                items=None,
                attachments=None,
                utility_code=self.utility_tariff_code,
                third_party_expected=third_party_expected,
                service_id=self.service_id,
                utility="utility:sce",
            )
            log.debug("created %s", datum)
            billing_objects.append(datum)

        detect_and_close_modal(self._driver)
        log.info("created %s billing objects", len(billing_objects))
        log.debug("billing_objects=%s", billing_objects)
        self.billing_history = billing_objects

        # Go back to home page in case a generation account is specified
        self._driver.get("https://www.sce.com/mysce/myaccount")

    def view_generation_usage_action(
        self, page: sce_pages.SceBilledGenerationUsageModal
    ):
        """Scrape generation bill data; these are displayed on a different modal than bundled/T&D bills."""
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
                    utility_code=self.utility_tariff_code,
                    service_id=self.gen_service_id,
                    utility="utility:clean-power-alliance",
                )
            )
        log.info("created %s generation billing objects", len(gen_billing_objects))
        log.debug("gen_billing_objects=%s", gen_billing_objects)
        self.gen_billing_history = gen_billing_objects


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
    metascraper=False,
) -> Status:
    # If there's a generation service id for the meter, get generation partials (with gen_service_id)
    # and T&D partials (with service_id). Otherwise, get bundled bills.
    is_partial = meter.utility_service.gen_service_id is not None
    configuration = SceReactBasicBillingConfiguration(
        service_id=meter.service_id,
        gen_service_id=meter.utility_service.gen_service_id,
        utility_account_id=meter.utility_service.utility_account_id,
        scrape_bills=not is_partial,
        scrape_partial_bills=is_partial,
        metascraper=metascraper,
        account_datasource_id=datasource._account_data_source,
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
