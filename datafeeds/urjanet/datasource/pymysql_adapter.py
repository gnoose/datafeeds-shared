"""An urjanet data source using the pymysql library

The primary class of use from this module is the UrjanetPyMySqlDataSource,
which defines how to populate an Urjanet data model using queries against
a MySQL database via pymysql.

SqlAlchemy may also be a good fit here, but the SQL queries required are
sufficiently simple that using pymysql is not terribly onerous.
"""

from abc import abstractmethod
from decimal import Decimal
from datetime import date
from datetime import datetime
from typing import List, Dict, Callable, Any, Type

from pymysql.cursors import DictCursor

from . import UrjanetDataSource
from ..model import (UrjanetData, Account, Meter, Usage, Charge)

SqlRowDict = Dict[str, Any]
SqlQueryResult = List[SqlRowDict]
Transform = Callable[[Any], Any]


def get_column(
        row: SqlRowDict,
        colname: str,
        transform: Transform = None,
        enforce_type: Type = None,
        nullable: bool = True) -> Any:
    """Pull a value out of a pymysql DictCursor row, with various constraints

    Args:
        row: A row returned from a DictCursor.
        colname: The name of the column to extract
        transform: A function to apply to the extracted value. This happens
            before any type check is applied. None by default. If this
            transform fails, a ValueError is thrown.
        enforce_type: A python class that should match the type of the
            extracted value, post transformation. None by default. If the type
            doesn't match, a TypeError is thrown
        nullable: Can the column value be None? If False, a ValueError is
            thrown if the value is None.

    Returns:
       The value of the given column in the given DictCursor row, transformed
       by the given function.

    Raises:
       KeyError: When "colname" isn't present in the dict
       ValueError: When (1) nullable=True and the value is None;
           (2) When transform is specified and causes an exception
       TypeError: When enforce_type is passed a class, and the
           (transformed) value doesn't match
    """

    # This will raise a KeyError if the given column isn't present
    value = row[colname]

    # Return None values directly
    if value is None:
        if nullable:
            return None
        raise ValueError("Column '{}' was unexpectedly None".format(colname))

    # Transform the value if requested, raising a ValueError on failure
    if transform:
        try:
            value = transform(value)
        except Exception as e:
            msg = "Failed to transform column '{}'".format(colname)
            raise ValueError(msg) from e

    # Check the type if requested, raising a TypeError on failure
    if enforce_type and not isinstance(value, enforce_type):
        actual_type = type(value)
        raise TypeError("Column '{}' should be type '{}', but was '{}'".format(
            colname, enforce_type.__name__, actual_type.__name__))
    return value


def get_int(row: SqlRowDict, colname: str, transform: Transform = int, nullable: bool = True) -> int:
    """Extract an integer value from a query result"""
    return get_column(
        row, colname, enforce_type=int, transform=transform, nullable=nullable)


def get_bool(row: SqlRowDict, colname: str, transform: Transform = bool, nullable: bool = True) -> bool:
    """Extract a boolean value from a query result"""
    return get_column(
        row,
        colname,
        enforce_type=bool,
        transform=transform,
        nullable=nullable)


def get_str(row: SqlRowDict, colname: str, transform: Transform = str, nullable: bool = True) -> str:
    """Extract a string value from a query result"""
    return get_column(
        row, colname, enforce_type=str, transform=transform, nullable=nullable)


def date_transform(value: Any) -> date:
    """Convert a value to a date

    Must already be a datetime or date object.
    """
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    raise ValueError("Invalid date value: {}".format(value))


def get_date(row: SqlRowDict, colname: str, transform: Transform = date_transform, nullable: bool = True) -> date:
    """Extract a date value from a query result"""
    return get_column(
        row,
        colname,
        enforce_type=date,
        transform=transform,
        nullable=nullable)


def get_decimal(row: SqlRowDict, colname: str, transform: Transform = Decimal, nullable: bool = True) -> Decimal:
    """Extract a decimal value from a query result"""
    return get_column(
        row,
        colname,
        enforce_type=Decimal,
        transform=transform,
        nullable=nullable)


class UrjanetPyMySqlDataSource(UrjanetDataSource):
    """Load data from a MySQL database into a data model.

    This implementation is currently an abstract class. The method for selecting
    appropriate "Account" and "Meter" rows from the SQL database must be
    specified by implementers via the "load_accounts" and "load_meters"
    functions. The justification for this design is that different utilities
    may require different logic to correctly identify these rows.
    """

    def __init__(self, account_number: str):
        """Initialize a datasource with a pymysql connection object."""
        super().__init__()
        self.account_number = account_number
        self.conn = None  # must set this before using

    def fetch_all(self, query: str, *argv) -> SqlQueryResult:
        """Helper function for executing a query and fetching all results"""
        with self.conn.cursor(DictCursor) as cursor:
            cursor.execute(query, tuple(argv))
            return cursor.fetchall()

    @abstractmethod
    def load_accounts(self) -> List[Account]:
        """The query for fetching accounts must be provided by implementers"""
        pass

    @abstractmethod
    def load_meters(self, account_pk: int) -> List[Meter]:
        """The query for fetching meters must be provided by implementers"""
        pass

    def load_meter_charges(self, account_pk: int, meter_pk: int) -> List[Charge]:
        """Fetch all charge info for a given meter"""
        query = """
            SELECT *
            FROM Charge
            WHERE AccountFK=%s AND MeterFK=%s
        """
        result_set = self.fetch_all(query, account_pk, meter_pk)
        return [
            UrjanetPyMySqlDataSource.parse_charge_row(row)
            for row in result_set
        ]

    def load_meter_usages(self, account_pk: int, meter_pk: int) -> List[Usage]:
        """Fetch all usage info for a given meter"""
        query = """
            SELECT *
            FROM `Usage`
            WHERE AccountFK=%s AND MeterFK=%s
        """
        result_set = self.fetch_all(query, account_pk, meter_pk)
        return [
            UrjanetPyMySqlDataSource.parse_usage_row(row) for row in result_set
        ]

    def load_floating_charges(self, account_pk: int) -> List[Charge]:
        """Floating charges are charges on a statement attached to no meter"""
        query = """
            SELECT *
            FROM Charge
            WHERE AccountFK=%s AND MeterFK is null
        """
        result_set = self.fetch_all(query, account_pk)
        return [
            UrjanetPyMySqlDataSource.parse_charge_row(row)
            for row in result_set
        ]

    def load(self) -> UrjanetData:
        """Load Urjanet data from the MySQL connection.

        The exact quantity of data loaded here depends on how implementers
        of this class define "load_accounts" and "load_meters". The idea
        is that implementers should provide mechanisms for filtering the
        data retrieved.
        """
        accounts = self.load_accounts()
        for account in accounts:
            meters = self.load_meters(account.PK)
            account.meters.extend(meters)

            floating_charges = self.load_floating_charges(account.PK)
            account.floating_charges.extend(floating_charges)

            for meter in meters:
                charges = self.load_meter_charges(account.PK, meter.PK)
                usages = self.load_meter_usages(account.PK, meter.PK)
                meter.charges.extend(charges)
                meter.usages.extend(usages)

        accounts_with_data = [a for a in accounts if len(a.meters) > 0 or len(a.floating_charges) > 0]
        return UrjanetData(accounts=accounts_with_data)

    @staticmethod
    def parse_account_row(row: SqlRowDict) -> Account:
        """Convert a query result row into an Urjanet Account object"""
        return Account(
            PK=get_int(row, "PK", nullable=False),
            UtilityProvider=get_str(row, "UtilityProvider"),
            AccountNumber=get_str(row, "AccountNumber"),
            RawAccountNumber=get_str(row, "RawAccountNumber"),
            SourceLink=get_str(row, "SourceLink"),
            StatementType=get_str(row, "StatementType"),
            StatementDate=get_date(row, "StatementDate"),
            IntervalStart=get_date(row, "IntervalStart"),
            IntervalEnd=get_date(row, "IntervalEnd"),
            TotalBillAmount=get_decimal(row, "TotalBillAmount"),
            AmountDue=get_decimal(row, "AmountDue"),
            NewCharges=get_decimal(row, "NewCharges"),
            OutstandingBalance=get_decimal(row, "OutstandingBalance"),
            PreviousBalance=get_decimal(row, "PreviousBalance"),
            meters=[],
            floating_charges=[])

    @staticmethod
    def parse_meter_row(row: SqlRowDict) -> Meter:
        """Convert a query result row into an Urjanet Meter object"""
        return Meter(
            PK=get_int(row, "PK", nullable=False),
            Tariff=get_str(row, "Tariff"),
            ServiceType=get_str(row, "ServiceType"),
            PODid=get_str(row, "PODid"),
            MeterNumber=get_str(row, "MeterNumber"),
            IntervalStart=get_date(row, "IntervalStart"),
            IntervalEnd=get_date(row, "IntervalEnd"),
            charges=[],
            usages=[])

    @staticmethod
    def parse_charge_row(row: SqlRowDict) -> Charge:
        """Convert a query result row into an Urjanet Charge object"""
        return Charge(
            PK=get_int(row, "PK", nullable=False),
            ChargeActualName=get_str(row, "ChargeActualName"),
            ChargeAmount=get_decimal(row, "ChargeAmount"),
            UsageUnit=get_str(row, "UsageUnit"),
            ChargeUnitsUsed=get_decimal(row, "ChargeUnitsUsed"),
            ChargeRatePerUnit=get_decimal(row, "ChargeRatePerUnit"),
            ThirdPartyProvider=get_str(row, "ThirdPartyProvider"),
            IsAdjustmentCharge=get_bool(row, "IsAdjustmentCharge"),
            IntervalStart=get_date(row, "IntervalStart"),
            IntervalEnd=get_date(row, "IntervalEnd"),
            ChargeId=get_str(row, "ChargeId"))

    @staticmethod
    def parse_usage_row(row: SqlRowDict) -> Usage:
        """Convert a query result row into an Urjanet Usage object"""
        return Usage(
            PK=get_int(row, "PK", nullable=False),
            UsageActualName=get_str(row, "UsageActualName"),
            UsageAmount=get_decimal(row, "UsageAmount"),
            RateComponent=get_str(row, "RateComponent"),
            EnergyUnit=get_str(row, "EnergyUnit"),
            IntervalStart=get_date(row, "IntervalStart"),
            IntervalEnd=get_date(row, "IntervalEnd"))
