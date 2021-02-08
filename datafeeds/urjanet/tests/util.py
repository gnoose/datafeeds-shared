import csv
import json
import logging
import os
import re
import unittest
from datetime import date
from decimal import Decimal
from typing import NamedTuple, Dict, List

from dateutil import parser as date_parser

from datafeeds.urjanet.model import (
    Account,
    Meter,
    Usage,
    Charge,
    GridiumBillingPeriodCollection,
    GridiumBillingPeriod,
)
from datafeeds.urjanet.model import filter_by_date_range
from datafeeds.urjanet.datasource.base import UrjanetDataSource
from datafeeds.urjanet.transformer import json_to_urja, UrjanetGridiumTransformer


log = logging.getLogger(__name__)


class FixtureDataSource(UrjanetDataSource):
    """An urjanet data source that loads data from a json file"""

    def __init__(self, fixture_path):
        super().__init__()
        self.fixture_path = fixture_path
        self.utility = "utility:default"
        self.account_number = "123"

    def load(self):
        with open(self.fixture_path) as f:
            data = json.load(f)
            return json_to_urja(data)


class FixtureRow(NamedTuple):
    utility: str
    utility_account_id: str
    service_id: str
    start: date
    end: date
    cost: float
    used: float
    peak: float


class UrjaCsvFixtureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data_directory = os.path.join(os.path.split(__file__)[0], "data")

    def load_fixture(self, utility: str) -> Dict[str, List[FixtureRow]]:
        """Read expected results from datafeeds/urjanet/tests/data/utility_id.csv

        Return a map from service_id or utility_account_id to a list of bills.
        Use service_id if set, otherwise utility_account_id.
        Electric meters usually do have a service_id, but water meters usually don't.
        """
        by_key: Dict[str, List[FixtureRow]] = {}
        with open("%s/%s.csv" % (self.data_directory, utility)) as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row["service_id"] or row["utility_account_id"]
                by_key.setdefault(key, [])
                by_key[key].append(
                    FixtureRow(
                        utility=row["utility"],
                        utility_account_id=row["utility_account_id"],
                        service_id=row["service_id"],
                        start=date_parser.parse(row["start"]).date(),
                        end=date_parser.parse(row["end"]).date(),
                        cost=float(row["cost"]) if row.get("cost") != "" else None,
                        used=float(row["used"]) if row.get("used") != "" else None,
                        peak=float(row["peak"]) if row.get("peak") != "" else None,
                    )
                )
        return by_key

    def match_bill(
        self, bills: List[GridiumBillingPeriod], bill: FixtureRow, label
    ) -> None:
        """Match a fixture bill to a bill in the list of billing periods created by the transformer."""
        match: GridiumBillingPeriod = None
        for billing_period in bills:
            if bill.end == billing_period.end:
                match = billing_period
                break
        self.assertTrue(match, "%s\tfound bill for %s" % (label, bill.end))
        self.assertEqual(bill.start, match.start, "%s\tstart date match" % label)
        self.assertAlmostEqual(
            bill.cost, float(match.total_charge), 2, "%s\tcost match" % label
        )
        self.assertAlmostEqual(
            bill.used, float(match.total_usage), 2, "%s\tused match" % label
        )
        if bill.peak is None:
            self.assertIsNone(match.peak_demand, "%s\tno peak" % label)
        else:
            self.assertAlmostEqual(
                bill.peak, float(match.peak_demand), 2, "%s\tpeak match" % label
            )

    def verify_transform(self, transformer: UrjanetGridiumTransformer, utility: str):
        """Verify a transformer by comparing against a csv fixture.

        csv data should be datafeeds/urjanet/tests/data/utility-id.csv
        with fields utility,utility_account_id,service_id,start,end,cost,used,peak
        """
        fixture = self.load_fixture(utility)
        for key in fixture:
            # read data from data/utility_id/key.json
            input_data_source = FixtureDataSource(
                "%s/%s/%s.json" % (self.data_directory, utility.replace("-", "_"), key)
            )
            input_data = input_data_source.load()
            # run transformer
            output = transformer.urja_to_gridium(input_data)
            log.debug("created %s bills" % len(output.periods))
            for period in output.periods:
                log.debug(
                    "%s - %s\t%.2f\t%.2f",
                    period.start,
                    period.end,
                    period.total_charge,
                    period.total_usage,
                )
            for bill in fixture[key]:
                log.debug(
                    "looking for bill %s - %s\t%s\t%s",
                    bill.start,
                    bill.end,
                    bill.cost,
                    bill.used,
                )
                self.match_bill(
                    output.periods,
                    bill,
                    "%s %s" % (bill.utility_account_id, bill.service_id),
                )


class UrjaFixtureText(unittest.TestCase):
    def load_expected_results(self, path):
        with open(path) as f:
            json_dict = json.load(f)
        return GridiumBillingPeriodCollection(json_dict)

    def fixture_test(
        self, transformer, input_path, expected_path, start_date=None, end_date=None
    ):
        input_urja_data = FixtureDataSource(input_path).load()
        fixture = re.sub(".*?/tests/data", "data", input_path)
        if start_date or end_date:
            input_urja_data = filter_by_date_range(
                input_urja_data, after=start_date, before=end_date
            )
        expected_result = self.load_expected_results(expected_path)
        actual_result = transformer.urja_to_gridium(input_urja_data)

        self.assertEqual(
            len(expected_result.periods),
            len(actual_result.periods),
            "%s periods" % fixture,
        )
        expected_sorted = sorted(expected_result.periods, key=lambda p: p.start)
        result_sorted = sorted(actual_result.periods, key=lambda p: p.start)
        for (e, r) in zip(expected_sorted, result_sorted):
            self.assertEqual(e.start, r.start, "%s start" % fixture)
            self.assertEqual(e.end, r.end, "%s end" % fixture)
            self.assertEqual(
                e.total_charge, r.total_charge, "%s total charge" % fixture
            )
            self.assertEqual(e.total_usage, r.total_usage, "%s total usage" % fixture)
            self.assertEqual(e.peak_demand, r.peak_demand, "%s peak demand" % fixture)
            self.assertEqual(e.tariff, r.tariff, "%s tariff" % fixture)


def default_usage(
    PK=1,
    UsageActualName="test_usage_actual_name",
    UsageAmount=Decimal(100.0),
    RateComponent="test_rate_component",
    EnergyUnit="test_energy_unit",
    IntervalStart=date(2000, 1, 1),
    IntervalEnd=date(2000, 2, 1),
):
    """Create an Urjanet Usage object with all fields filled in"""
    return Usage(
        PK=PK,
        UsageActualName=UsageActualName,
        UsageAmount=UsageAmount,
        RateComponent=RateComponent,
        EnergyUnit=EnergyUnit,
        IntervalStart=IntervalStart,
        IntervalEnd=IntervalEnd,
    )


def default_charge(
    PK=1,
    ChargeActualName="test_charge_actual_name",
    ChargeAmount=Decimal(100.0),
    UsageUnit="kW",
    ChargeUnitsUsed=Decimal(20.0),
    ChargeRatePerUnit=Decimal(5.0),
    ThirdPartyProvider="test_third_party_provider",
    IsAdjustmentCharge=False,
    IntervalStart=date(2000, 1, 1),
    IntervalEnd=date(2000, 2, 1),
):
    """Create an Urjanet Charge object with all fields filled in"""
    return Charge(
        PK=PK,
        ChargeActualName=ChargeActualName,
        ChargeAmount=ChargeAmount,
        UsageUnit=UsageUnit,
        ChargeUnitsUsed=ChargeUnitsUsed,
        ChargeRatePerUnit=ChargeRatePerUnit,
        ThirdPartyProvider=ThirdPartyProvider,
        IsAdjustmentCharge=IsAdjustmentCharge,
        IntervalStart=IntervalStart,
        IntervalEnd=IntervalEnd,
    )


def default_meter(
    PK=1,
    Tariff="test_tariff",
    ServiceType="test_service_type",
    PODid="test_podid",
    MeterNumber="test_meter_number",
    IntervalStart=date(2000, 1, 1),
    IntervalEnd=date(2000, 2, 1),
    charges=None,
    usages=None,
):
    """Create an Urjanet Meter object with all fields filled in"""
    return Meter(
        PK=PK,
        Tariff=Tariff,
        ServiceType=ServiceType,
        PODid=PODid,
        MeterNumber=MeterNumber,
        IntervalStart=IntervalStart,
        IntervalEnd=IntervalEnd,
        charges=[] if not charges else charges,
        usages=[] if not usages else usages,
    )


def default_account(
    PK=1,
    UtilityProvider="test_provider",
    AccountNumber="test_account_number",
    RawAccountNumber="test_raw_account_number",
    SourceLink="test_source_link",
    StatementType="test_statement_type",
    StatementDate=date(2000, 2, 5),
    IntervalStart=date(2000, 1, 1),
    IntervalEnd=date(2000, 2, 1),
    TotalBillAmount=Decimal(100.0),
    AmountDue=Decimal(90.0),
    NewCharges=Decimal(80.0),
    OutstandingBalance=Decimal(70.0),
    PreviousBalance=Decimal(60.0),
    meters=None,
    floating_charges=None,
):
    """Create an Urjanet Account object with all fields filled in"""
    return Account(
        PK=PK,
        UtilityProvider=UtilityProvider,
        AccountNumber=AccountNumber,
        RawAccountNumber=RawAccountNumber,
        SourceLink=SourceLink,
        StatementType=StatementType,
        StatementDate=StatementDate,
        IntervalStart=IntervalStart,
        IntervalEnd=IntervalEnd,
        TotalBillAmount=TotalBillAmount,
        AmountDue=AmountDue,
        NewCharges=NewCharges,
        OutstandingBalance=OutstandingBalance,
        PreviousBalance=PreviousBalance,
        meters=[] if not meters else meters,
        floating_charges=[] if not floating_charges else floating_charges,
    )
