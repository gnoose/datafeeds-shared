"""This module captures exception types that commonly arise in the course of scraping a website or api."""


class LoginError(Exception):
    pass


class ApiError(Exception):
    pass


class InvalidDateRangeError(Exception):
    pass


class DataSourceConfigurationError(Exception):
    pass
