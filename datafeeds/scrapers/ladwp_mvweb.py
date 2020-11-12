from datetime import timedelta, date
import logging
from typing import Optional

from datafeeds import db
from datafeeds.common.base import CSSSelectorBasePageObject
from datafeeds.common.batch import run_datafeed, iso_to_dates
from datafeeds.common.exceptions import LoginError
from datafeeds.common.support import Configuration as BaseConfiguration, DateRange
from datafeeds.common.typing import Status
from datafeeds.models import SnapmeterAccount, Meter, SnapmeterMeterDataSource
from datafeeds.models.meter import MeterReading
from datafeeds.scrapers.heco_interval import HECOScraper

log = logging.getLogger(__name__)


class Configuration(BaseConfiguration):
    def __init__(self, mvweb_id: str, interval: int):
        super().__init__(scrape_readings=True)
        self.mvweb_id = mvweb_id
        self.interval = interval


class LoginPage(CSSSelectorBasePageObject):
    """Represents the authenticate page for LADWP - MV-WEB Client

    A very basic login page with username and password fields.
    """

    UsernameFieldSelector = "input#loginUserName"
    PasswordFieldSelector = "input#loginPassword"
    SigninButtonSelector = "button#loginButton"
    LandingPageIndicator = 'a[href="/mvwebcl/meters"]'
    ErrorSelector = "#smallbox1 > div.textoFull"

    def get_signin_button(self):
        return self.find_element(self.SigninButtonSelector)

    def login(self, username: str, password: str):
        """Authenticate with the web page.

        Fill in the username, password, then click "Log in"
        """
        log.info("Inserting credentials on login page.")
        self._driver.fill(self.UsernameFieldSelector, username)
        self._driver.fill(self.PasswordFieldSelector, password)
        self.get_signin_button().click()

        self.wait_until_ready(
            self.LandingPageIndicator,
            error_selector=self.ErrorSelector,
            error_cls=LoginError,
            error_msg="Login error.",
        )


class LADWPMVWebScraper(HECOScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "LADWP-MVWeb Selenium"
        self.login_url = "https://mvweb.ladwp.com/mvwebcl/auth/login"
        # Download link selector can differ on MVWeb implementation
        self.download_link_selector = 'a[href="/mvwebcl/download"]'

    @property
    # Overrides HECOScraper - meter_id in meta rather than service_id
    def meter_id(self):
        return self._configuration.mvweb_id

    # Overrides HECOScraper - different entry point into MVWeb
    def login_to_mvweb(self):
        login_page = LoginPage(self._driver)
        login_page.wait_until_ready(login_page.UsernameFieldSelector)
        self.screenshot("before login")
        login_page.login(self.username, self.password)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: SnapmeterMeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    meta = datasource.meta or {}
    configuration = Configuration(mvweb_id=meta.get("mvWebId"), interval=meter.interval)
    # reduce load on MVWeb servers: skip if meter has data from within the last 3 days and there are no gaps
    max_reading = meter.readings_range.max_date or date.today() - timedelta(days=365)
    interval_age = (date.today() - max_reading).days
    date_range = DateRange(
        *iso_to_dates(params.get("data_start"), params.get("data_end"))
    )
    # freshest we can expect is 3 days old
    date_range = DateRange(
        date_range.start_date,
        min(date_range.end_date, date.today() - timedelta(days=3)),
    )
    expected = (date_range.end_date - date_range.start_date).days + 1
    days_with_data = (
        db.session.query(MeterReading)
        .filter(
            MeterReading.meter == meter.oid,
            MeterReading.occurred >= date_range.start_date,
            MeterReading.occurred <= date_range.end_date,
        )
        .count()
    )
    log.info(
        "days with data from %s - %s = %s",
        date_range.start_date,
        date_range.end_date,
        days_with_data,
    )
    if interval_age <= 3 and days_with_data == expected:
        log.info(
            "skipping MVWeb run: meter %s has recent interval data (%s) and no gaps",
            meter.oid,
            max_reading,
        )
        return Status.SKIPPED

    return run_datafeed(
        LADWPMVWebScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
