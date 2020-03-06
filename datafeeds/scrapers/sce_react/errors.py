"""Custom exceptions raised by SCE react scrapers"""


class LoginFailedException(Exception):
    """Indicates a general login failure.

    Users should try to provide context on the reason for failure, if possible.
    """

    pass


class IntervalDataParseException(Exception):
    """Indicates an error during parsing raw interval data obtained from the SCE website"""

    pass


class EnergyManagerDataNotFoundException(Exception):
    """Indicates that interval data was not found on the SCE website"""

    pass


class EnergyManagerReportException(Exception):
    """Represents an error that occurs during interval data retrieval in the SCE EnergyManager tool"""

    pass


class BillingDataParseException(Exception):
    """Indicates an error during parsing raw billing data obtained from the SCE website"""

    pass


class BillingDataNotFoundException(Exception):
    """Indicates that billing data was not found on the SCE website"""

    pass


class BillingDataDateRangeException(Exception):
    """Indicates that billing data was not found on the SCE website"""

    pass


class ServiceIdException(Exception):
    """Indicates an error related to the service ID specified to a scraper run

    E.g. a missing or otherwise invalid service ID.
    """

    pass
