import argparse
from typing import List, Dict, Optional

from datetime import timedelta
from dateutil.relativedelta import relativedelta

from datafeeds.common.typing import BillPdf, BillingDatum


class ScraperArgs:
    """Most scrapers require the same sets of arguments, so provide a sane set of defaults for testing.

    Subclasses can still add both positional and optional args or call any other method on the ArgumentParser.
    """

    def __init__(self, *args, **kwargs):
        self._parser = argparse.ArgumentParser(*args, **kwargs)
        self._set_default_args()

    def __getattr__(self, attr):
        """Proxy calls to ArgumentParser"""
        return getattr(self._parser, attr)

    def _set_default_args(self):
        """Set all common args for scrapers"""
        self._parser.add_argument("username")
        self._parser.add_argument("password")
        self._parser.add_argument(
            "--start",
            help="Start date for the scraper in iso format, eg: 2017-11-19",
            type=str,
            default=None,
        )
        self._parser.add_argument(
            "--end",
            help="End date for the scraper in iso format",
            type=str,
            default=None,
        )
        self._parser.add_argument(
            "--skip-delete",
            help="Delete the scraper folder in /tmp after run",
            action="store_true",
        )

    def parse(self):
        return self._parser.parse_args()


class Configuration:
    """Container for customizable scraper options - inherit to add
    configuration for specific scraper runs, such as options from datasource
    or account/service ID

    Partial billing scrapers should return Results with data in either tnd_bills or generation_bills.
    """

    def __init__(
        self,
        scrape_bills: bool = False,
        scrape_readings: bool = False,
        scrape_pdfs: bool = False,
        scrape_partial_bills: bool = False,
    ):
        self.scrape_bills = scrape_bills
        self.scrape_readings = scrape_readings
        self.scrape_pdfs = scrape_pdfs
        self.scrape_partial_bills = scrape_partial_bills


class Credentials:
    """Container for data source credentials"""

    def __init__(self, username, password):
        self.username = username
        self.password = password


class DateRange:
    """Container for start & end dates that allows for iterating through the range inclusively."""

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def __eq__(self, other) -> bool:
        if not isinstance(other, DateRange):
            return NotImplemented

        return self.start_date == other.start_date and self.end_date == other.end_date

    def __iter__(self):
        current = self.start_date
        while current <= self.end_date:
            yield current
            current += relativedelta(days=1)

    def __str__(self):
        return "{} - {}".format(self._fmt(self.start_date), self._fmt(self.end_date))

    def split_iter(self, delta):
        """Split the range into evenly-sized time intervals

        Args:
            delta: A dateutil.relativedelta instance
        """
        interval_start = self.start_date
        while interval_start < self.end_date:
            interval_end = interval_start + delta
            if interval_end > self.end_date:
                interval_end = self.end_date
            yield DateRange(interval_start, interval_end)
            interval_start = interval_end + relativedelta(days=1)

    @staticmethod
    def _fmt(dt):
        return dt.strftime("%m/%d/%Y")

    def intersects(self, other: "DateRange") -> bool:
        """
        Determine if two DateRanges intersect
        """
        return (
            self.end_date - other.start_date
            >= timedelta(0)
            >= self.start_date - other.end_date
        )


class Results:
    """Container for bill, interval, or pdf results

    Readings should a dictionary like this:
        {
            '%Y-%m-%d' : [ N float or null ]
        }
    """

    def __init__(
        self,
        bills: List[BillingDatum] = None,
        readings: Dict[str, List[Optional[float]]] = None,
        pdfs: List[BillPdf] = None,
        generation_bills: List[BillingDatum] = None,
        tnd_bills: List[BillingDatum] = None,
    ):
        self.bills = bills
        self.generation_bills = generation_bills
        self.tnd_bills = tnd_bills
        self.readings = readings
        self.pdfs = pdfs
