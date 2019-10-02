"""Core Urjanet data model

The classes defined in this module represent a projection of the
(relational) Urjanet data model into (non-relational) python
objects. A subset of fields are taken from each Urjanet entity
(Account, Meter, Charge, and Usage), and foreign-key relationships are
expressed as collections.

Generally speaking, field names correspond to the names of columns in
the Urjanet MySQL schema, including the Pascal casing
(e.g. "UsageActualName"), Collection fields are all lower-case
(e.g. "meters", "charges")

Additionally, this module contains a handful of classes for
representing the products of Urjanet data transformation, principally
the transformation from raw Urjanet entities to billing periods.

The primary goal of this data model is to define intermediate objects
to faciliate the transformation of Urjanet data to other formats
(e.g. Gridium billing periods).

Currently, the jsonobject library is used to define these types
(https://github.com/dimagi/jsonobject). This is a lightweight
"ORM-like" system, where, conceptually, the backing store is a json
document. The primary justification for this is testing: one can
express a complex test case as a json document, and easily
deserialized it into this data model.
"""

import copy
from collections import OrderedDict
from typing import Any, Callable
from datetime import date

# pylint: disable=E0611
from jsonobject import (
    JsonObject,
    DecimalProperty,
    StringProperty,
    DateProperty,
    IntegerProperty,
    ListProperty,
    BooleanProperty
)


class Usage(JsonObject):
    """An Urjanet "Usage" object.

    From Urjanet's documentation:
      | The Usage table stores commodity-specific consumption data.
      | Here, you will find information such as consumption amounts,
      | read types, present and previous meter readings, and units
      | of measure.
    """
    # The primary key of the Usage object in the Urjanet database
    PK = IntegerProperty(required=True)

    # A string description of the usage measurement
    UsageActualName = StringProperty()

    # The measured amount of usage
    UsageAmount = DecimalProperty()

    # The rate component, e.g. "on peak", "off peak", etc.
    RateComponent = StringProperty()

    # Unit of measure, typically kW or kWh
    EnergyUnit = StringProperty()

    # The start date for the usage measurement
    IntervalStart = DateProperty()

    # The end date for the usage measurement
    IntervalEnd = DateProperty()


class Charge(JsonObject):
    """An Urjanet "Charge" object.

    From the Urjanet documentation:
      | The Charge table includes information related to the
      | line-item charges on your utility bill. Here, you can
      | find details such as charges names, rates, and currencies.
      | If a charge is associated with more than one meter, Urjanet
      | will prorate the charge based on the usage amount.

    Note especially the description of proration, which can
    manifest in some somewhat difficult to interpret data.
    """
    # The primary key of the Charge object in the Urjanet database
    PK = IntegerProperty(required=True)

    # The line item name
    ChargeActualName = StringProperty()

    # The charge amount in dollars
    ChargeAmount = DecimalProperty()

    # The usage associated with this charge, if any
    ChargeUnitsUsed = DecimalProperty()

    # The units on the usage associated with this charge
    UsageUnit = StringProperty()

    # The rate associated with this charge (e.g. price per unit of usage)
    ChargeRatePerUnit = DecimalProperty()

    # The name of any third party provider (e.g. a CCA entity)
    # This is useful for identifying CCA charges
    ThirdPartyProvider = StringProperty()

    # TODO: Explain this flag in more detail. It may be a somewhat
    # misleading field. For now, avoid using it.
    IsAdjustmentCharge = BooleanProperty()

    # The start date for the period the charge applies to
    IntervalStart = DateProperty()

    # The end date for the period the charge applies to
    IntervalEnd = DateProperty()


class Meter(JsonObject):
    """An Urjanet "Meter" object

    From Urjanet's documentation:
      | The Meter table organizes usage information into specific
      | "points" of service. Urjanet's definition of a meter is not
      | limited to physical meters. The meter table captures data
      | such as service types, tariffs, and usage periods.
    """

    # The primary key of the Meter object in the Urjanet database
    PK = IntegerProperty(required=True)

    # The tariff assigned to the meter.
    # Note: this is drawn from text in a PDF, and is not guaranteed
    # to have a consistent format (that is, tariff's on separate meters
    # cannot reliably be compared in a direct fashion)
    Tariff = StringProperty()

    # The service type of the meter (e.g. electric, natural_gas)
    ServiceType = StringProperty()

    # The "Point of Delivery" identifier for the meter. Interpretation
    # of this field varies for different utilities. For Pacific Gas &
    # Electric, this corresponds to a service ID.
    PODid = StringProperty()

    # The meter number for the meter. Interpretation of this field varies
    # for different utilities.
    MeterNumber = StringProperty()

    # The start/end dates for the Meter object. These fields are a little
    # difficult to interpret. Roughly speaking, they define, respectively, the
    # earliest and latest date for which usage/charge information is available
    # for the meter on a given statement. While this is usually set correctly,
    # there are times where individual charge/usage date ranges do not fully
    # interect with their parent meter.
    IntervalStart = DateProperty()
    IntervalEnd = DateProperty()

    # The charges associated with this meter
    charges = ListProperty(Charge)

    # The usage values associated with this meter
    usages = ListProperty(Usage)


class Account(JsonObject):
    """An Urjanet "Account" object

    From Urjanet documentation:
      | The Account table includes high-level information about an account on
      | a bill. This is where you will find important payment data such as the
      | amount due. This table also houses address data, such as billing and
      | payment addresses.

    For our purposes, Account is a synonym for "Billing Statement"
    """
    # The primary key of the Account object in the Urjanet database
    PK = IntegerProperty(required=True)

    # The name of the utility company (often shortened, e.g. "PacGAndE")
    UtilityProvider = StringProperty()

    # The account number with no preprocessing (e.g. to remove dashes)
    RawAccountNumber = StringProperty()

    # The preprocessed/cleaned account number (e.g. dashes removed for PG&E)
    AccountNumber = StringProperty()

    # A link to the source material for the statement (often a PDF)
    SourceLink = StringProperty()

    # The statement type; typically, "statement_type_bill" or
    # "statement_type_adjustment". The latter is a little misleading,
    # because a statement can contain both adjustments and novel charges,
    # so take care when using this field
    StatementType = StringProperty()

    # The date on which the statement was issued
    StatementDate = DateProperty()

    # The earliest start date for charges/usages appearing on the statement
    # (may be approximate)
    IntervalStart = DateProperty()

    # The latest start date for charges/usages appearing on the statement
    # (may be approximate)
    IntervalEnd = DateProperty()

    # The sum of charges for the account on the statement
    TotalBillAmount = DecimalProperty()

    # The amount due in this billing cycle
    AmountDue = DecimalProperty()

    # TODO: get clarification on how to interpret this field
    NewCharges = DecimalProperty()

    # TODO: get clarification on how to interpret this field
    OutstandingBalance = DecimalProperty()

    # TODO: get clarification on how to interpret this field
    PreviousBalance = DecimalProperty()

    # The list of meters associated with this account
    meters = ListProperty(Meter)

    # Charges associated with this account that are not associated with a meter
    floating_charges = ListProperty(Charge)


class UrjanetData(JsonObject):
    """A top-level object representing a collection of Urjanet data."""
    accounts = ListProperty(Account)


class GridiumBillingPeriod(JsonObject):
    """A gridium billing period synthesized from Urjanet data."""

    # The start date of the billing period
    start = DateProperty()

    # The end date of the billing period
    end = DateProperty()

    # The tariff associated with this period
    tariff = StringProperty()

    # A list of URLs pointing to the source documents for this billing period (e.g. PDF bills)
    source_urls = ListProperty(StringProperty)

    # The total charge for this period
    total_charge = DecimalProperty()

    # The peak demand for this period
    peak_demand = DecimalProperty()

    # The total usage for this period
    total_usage = DecimalProperty()

    # The list of charges for this period
    line_items = ListProperty(Charge)


class GridiumBillingPeriodCollection(JsonObject):
    """A top-level collection of billing periods."""
    periods = ListProperty(GridiumBillingPeriod)


# Some utility functions
def filter_urja_data(urjanet_data: UrjanetData,
                     account_filter: Callable[[Account], bool] = None,
                     meter_filter: Callable[[Meter], bool] = None,
                     charge_filter: Callable[[Charge], bool] = None,
                     usage_filter: Callable[[Usage], bool] = None) -> UrjanetData:
    """Filter a collection of Urjanet data by caller-defined criteria

    Filters are implemented by functions that accept the various Urjanet objects
    (Account, Meter, Charge, Usage) and return a bool (True if the object
    should be kept, False otherwise).

    Arguments:
        urjanet_data: The Urjanet data to filter
        account_filter: A function that accepts an Account and returns a bool
        meter_filter: A function that accepts a Meter and returns a bool
        charge_filter: A function that accepts a Charge and returns a bool
        usage_filter: A function that accepts a Usage and returns a bool

    Return:
        A copy of the given Urjanet data with the given filters applied
    """

    def null_filter(_):
        return True

    account_filter = account_filter if account_filter else null_filter
    meter_filter = meter_filter if meter_filter else null_filter
    charge_filter = charge_filter if charge_filter else null_filter
    usage_filter = usage_filter if usage_filter else null_filter

    accounts = [
        copy.copy(a) for a in urjanet_data.accounts if account_filter(a)
    ]
    for account in accounts:
        account.meters = [
            copy.copy(m) for m in account.meters if meter_filter(m)
        ]
        for meter in account.meters:
            meter.charges = [
                copy.copy(c) for c in meter.charges if charge_filter(c)
            ]
            meter.usages = [
                copy.copy(u) for u in meter.usages if usage_filter(u)
            ]
        account.floating_charges = [
            copy.copy(c) for c in account.floating_charges if charge_filter(c)
        ]
    return UrjanetData(accounts=accounts)


def filter_by_date_range(urjanet_data: UrjanetData, after: date = None, before: date = None) -> UrjanetData:
    """Filter Urjanet data by date

    The after/before filters apply to the IntervalStart field of Charge and Usage objects.

    Arguments:
        urjanet_data: The Urjanet data to filter
        after: The start date of charges/usages must be after this date (default: None)
        before: The start date of charges/usages must be before this date (default: None)

    Return:
        A copy of the given Urjanet data with the given filters applied
    """

    def in_range(elem):
        is_after = (after is None) or (elem.IntervalStart >= after)
        is_before = (before is None) or (elem.IntervalStart <= before)
        return is_after and is_before

    return filter_urja_data(
        urjanet_data, charge_filter=in_range, usage_filter=in_range)


def order_json(json_elem: Any) -> Any:
    """Convert a json dict into an OrderedDict

    The ordering of fields is as follows:
      - All non-dict, non-list fields appear first (e.g. strings, integers, etc.)
      - dict-fields appear next
      - list fields appear last
    """
    if isinstance(json_elem, dict):
        result = OrderedDict()
        lists = []
        dicts = []
        others = []

        for field in json_elem:
            value = json_elem[field]
            if isinstance(value, dict):
                dicts.append(field)
            elif isinstance(value, list):
                lists.append(field)
            else:
                others.append(field)

        for other_field in sorted(others):
            result[other_field] = json_elem[other_field]
        for dict_field in sorted(dicts):
            result[dict_field] = order_json(json_elem[dict_field])
        for list_field in sorted(lists):
            result[list_field] = [order_json(e) for e in json_elem[list_field]]
    elif isinstance(json_elem, list):
        result = [order_json(e) for e in json_elem]
    else:
        result = json_elem

    return result
