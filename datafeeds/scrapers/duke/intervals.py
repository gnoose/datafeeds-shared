""" Duke Interval scraper module """
from typing import Optional

from datafeeds.common.util.selenium import WindowSwitch
import datafeeds.scrapers.duke.pages as duke_pages
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Results
from datafeeds.common.typing import Status
from datafeeds.common.util.pagestate.pagestate import PageStateMachine
from datafeeds.scrapers.duke import errors
from datafeeds.scrapers import epo_schneider
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)


class DukeIntervalConfiguration(epo_schneider.EnergyProfilerConfiguration):
    def __init__(self, epo_meter_id: str, channel_id=None):
        base_url = None
        account_id = None
        log_in = False
        super().__init__(base_url, account_id, epo_meter_id, channel_id, log_in)


class DukeIntervalScraper(epo_schneider.EnergyProfilerScraper):
    """ Duke Interval scraper """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Duke Interval scraper"
        self.readings = None

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

        # We get to the landing page, where we need to open the EPO tool
        state_machine.add_state(
            name="main_landing_page",
            page=duke_pages.DukeLandingPage(self._driver),
            action=self.landing_page_action,
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
            return Results(readings=self.readings)

        raise errors.IntervalScraperException(
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

    def landing_page_action(self, page: duke_pages.DukeLandingPage):
        """Process landing page action """
        page.open_profiler_page()
        profiler_window = self._driver.window_handles[1]
        with WindowSwitch(self._driver, profiler_window):
            self.readings = super()._execute().readings


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = DukeIntervalConfiguration(
        meter.service_id, channel_id=datasource.meta.get("channelId", None)
    )

    return run_datafeed(
        DukeIntervalScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
