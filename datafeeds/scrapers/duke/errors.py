"""Custom exceptions for duke energy scrapers"""


class BillingScraperAccountUnavailable(Exception):
    """The requested account was not available."""

    pass


class BillingScraperException(Exception):
    """The scraper did not finish scraping"""

    pass


class BillingScraperInvalidDateRangeException(Exception):
    """The scraper received invalid date range parameters"""

    pass


class BillingScraperLoginException(Exception):
    """The scraper failed to log in"""

    pass


class BillingScraperFormatException(Exception):
    """Scraped data is in unexpected format"""

    pass


class BillingScraperPageNotFoundException(Exception):
    """Scraped data is in unexpected format"""

    pass


class IntervalScraperException(Exception):
    """The scraper did not finish scraping"""

    pass
