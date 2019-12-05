from abc import ABC as Abstract, abstractmethod
import csv
import os
import time
from typing import List
import logging

from datafeeds import config
from datafeeds.common.typing import BillingDatum
from datafeeds.common.support import Configuration
from datafeeds.common.webdriver.virtualdisplay import VirtualDisplay


log = logging.getLogger(__name__)


class UnsupportedBrowserError(Exception):
    pass


class BrowserConnectionError(Exception):
    pass


class BaseScraper(Abstract):
    """
    Base scraper that defines the general interface for more specialized scrapers
    to inherit. Makes no assumption about method of retrieving data (eg. scrape
    website vs. making API calls).

    Ideal use is as a context manager:

        with [Account]Scraper(...args) as scraper:
            interval_data = scraper.scrape().results

    Also makes no assumptions about whether data sources are configured or how
    the scraper results are handled.
    """

    # pylint: disable=too-many-instance-attributes
    # Vast majority of this state is immutable, and storing account id, etc
    # prevents need of having to manage them as parameters which can be quite
    # likely to change depending on the scraper

    @abstractmethod
    def _execute(self):
        pass

    def __init__(self, credentials, date_range, configuration=None):
        self.name = ""
        self._credentials = credentials
        self._date_range = date_range
        self._configuration = configuration or Configuration()

    @property
    def username(self):
        return self._credentials.username

    @property
    def password(self):
        return self._credentials.password

    @property
    def start_date(self):
        return self._date_range.start_date

    @start_date.setter
    def start_date(self, dt):
        self._date_range.start_date = dt

    @property
    def end_date(self):
        return self._date_range.end_date

    @end_date.setter
    def end_date(self, dt):
        self._date_range.end_date = dt

    @property
    def scrape_readings(self):
        return self._configuration.scrape_readings

    @property
    def scrape_bills(self):
        return self._configuration.scrape_bills

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    # FIXME: Refactor this to just return whatever bills/intervals were acquired.
    # Passing in the handlers would make sense if this function was responsible for handling
    # whatever exceptions they might throw. But since it raises and the caller is obliged to
    # wrap this in a try-catch, there's no benefit to the current interface.
    def scrape(self, readings_handler, bills_handler):
        log.info("Launching %s", self.name)
        if self.username:
            log.info("Username: %s", self.username)
        log.info("Date Range: %s - %s", self.start_date.strftime("%Y-%m-%d"), self.end_date.strftime("%Y-%m-%d"))
        log.info("Configuration:")
        for prop, value in vars(self._configuration).items():
            log.info("\t%s: %s", prop, value)

        try:
            results = self._execute()

            if self.scrape_bills:
                if results.bills:
                    bills_handler(results.bills)
                else:
                    log.error("Expected to find bills but none were returned.")

            if self.scrape_readings:
                if results.readings:
                    readings_handler(results.readings)
                else:
                    log.error("Expected to find interval data but none was returned.")

        except Exception:
            log.exception("Scraper run failed.")
            raise

    @staticmethod
    def log_bills(bills: List[BillingDatum]):
        if not bills:
            return
        path = os.path.join(config.WORKING_DIRECTORY, "bills.csv")
        with open(path, "w") as f:
            writer = csv.writer(f)
            writer.writerow(["start", "end", "cost", "used", "peak"])
            for bill in bills:
                data_row = [bill.start, bill.end, bill.cost, bill.used, bill.peak]
                writer.writerow(data_row)

    @staticmethod
    def log_readings(readings):
        if not readings:
            return
        path = os.path.join(config.WORKING_DIRECTORY, "readings.csv")
        with open(path, "w") as f:
            keys = sorted(readings.keys())
            writer = csv.writer(f)
            for key in keys:
                data_row = [key] + readings[key]
                writer.writerow(data_row)


class BaseApiScraper(BaseScraper):
    """
    Marker class for scrapers that only hit APIs and do not need to
    manage any resources
    """

    @abstractmethod
    def _execute(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass


class BaseWebScraper(BaseScraper):
    """
    Base scraper to handle common initialization, setup, and teardown
    shared between all web scrapers that run via a headless browser.

    Manages a VirtualDisplay and the browser driver (eg. chromedriver),
    including stopping processes once finished, and adds specialized
    methods like screenshotting.
    """

    @abstractmethod
    def _execute(self):
        pass

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._shot_number = 0

        if config.USE_VIRTUAL_DISPLAY:
            self._display = VirtualDisplay()

        self._driver = None
        # This allows classes extending base to select a different
        # browser for scraping.
        self.browser_name = config.SELENIUM_BROWSER

    def start(self):
        # Virtual display needs to be started before webdriver can be loaded
        if config.USE_VIRTUAL_DISPLAY:
            self._display.start()

        self._driver = self._get_driver()
        self._driver.start()

    def stop(self):
        self._driver.stop()
        if config.USE_VIRTUAL_DISPLAY:
            self._display.stop()

    def scrape(self, readings_handler, bills_handler):
        try:
            super().scrape(readings_handler, bills_handler)
        except Exception:
            self.screenshot("error")
            raise

    def screenshot(self, filename, whole=True):
        self._shot_number += 1
        path = os.path.join(
            config.WORKING_DIRECTORY,
            "screenshot{:02} - {}.png".format(self._shot_number, filename)
        )
        self._driver.screenshot(path, whole=whole)

    def _get_driver(self):
        """
        Return an instance of ChromeDriver trying several times to load the
        desired driver in case it takes awhile to connect to browser
        """

        # Drivers are references dynamically, so we need the imports
        from datafeeds.common.webdriver.drivers.chrome import ChromeDriver  # noqa

        browser = self.browser_name
        outputpath = config.WORKING_DIRECTORY

        if browser != "Chrome":
            raise UnsupportedBrowserError("Browser specified in config is not supported")

        for _ in range(1, 11):
            log.info("Connecting to {}".format(browser))

            try:
                return locals()["{}Driver".format(browser)](outputpath)
            except Exception as e:
                log.info("Failed to connect. Exception: %s" % repr(e))
                time.sleep(3)

        raise BrowserConnectionError("Unable to connect to {}".format(browser))
