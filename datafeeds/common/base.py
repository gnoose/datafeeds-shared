from abc import ABC as Abstract, abstractmethod
import csv
from typing import List
import logging

from datafeeds import config
from datafeeds.common.typing import BillingDatum
from datafeeds.common.support import Configuration


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

    def scrape(self, readings_handler, bills_handler):
        log.info("Launching {}".format(self.name, level="info"))
        log.info("Username:   {}".format(self.username))
        log.info("Start Date: {}".format(self._iso_str(self.start_date)))
        log.info("End Date:   {}".format(self._iso_str(self.end_date)))
        log.info("Configuration:")
        for prop, value in vars(self._configuration).items():
            log.info("\t{}: {}".format(prop, value))

        expected = lambda data: log.info(
            "Expected to find {} but none were returned".format(data),
            level="error"
        )

        try:
            results = self._execute()

            if self.scrape_bills:
                if results.bills:
                    bills_handler(results.bills)
                else:
                    expected("bills")

            if self.scrape_readings:
                if results.readings:
                    readings_handler(results.readings)
                else:
                    expected("readings")

        except Exception:
            log.exception("Scraper run failed.")
            raise

    def _with_path(self, filename):
        return "{}/{}".format(config.WORKING_DIRECTORY, filename)

    def log(self, msg="", level="debug"):
        getattr(log, level)(msg)

    def log_bills(self, bills: List[BillingDatum]):
        if not bills:
            return
        with open(self._with_path("bills.csv"), "w") as f:
            writer = csv.writer(f)
            writer.writerow(["start", "end", "cost", "used", "peak"])
            for bill in bills:
                data_row = [bill.start, bill.end, bill.cost, bill.used, bill.peak]
                writer.writerow(data_row)

    def log_readings(self, readings):
        if not readings:
            return
        with open(self._with_path("readings.csv"), "w") as f:
            keys = sorted(readings.keys())
            writer = csv.writer(f)
            for key in keys:
                data_row = [key] + readings[key]
                writer.writerow(data_row)

    @staticmethod
    def _iso_str(dt):
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def _date_str(dt):
        return dt.strftime("%m/%d/%Y")


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
