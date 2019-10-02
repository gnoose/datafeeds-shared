import json
import unittest
from datetime import date
from decimal import Decimal

from datafeeds.urjanet.model import (Account, Meter, Usage, Charge, GridiumBillingPeriodCollection)
from datafeeds.urjanet.model import filter_by_date_range
from datafeeds.urjanet.datasource import UrjanetDataSource
from datafeeds.urjanet.transformer import json_to_urja


class FixtureDataSource(UrjanetDataSource):
    """An urjanet data source that loads data from a json file"""

    def __init__(self, fixture_path):
        super().__init__()
        self.fixture_path = fixture_path

    def load(self):
        with open(self.fixture_path) as f:
            data = json.load(f)
            return json_to_urja(data)


class UrjaFixtureText(unittest.TestCase):
    def load_expected_results(self, path):
        with open(path) as f:
            json_dict = json.load(f)
        return GridiumBillingPeriodCollection(json_dict)

    def fixture_test(self, transformer, input_path, expected_path, start_date=None, end_date=None):
        input_urja_data = FixtureDataSource(input_path).load()
        if start_date or end_date:
            input_urja_data = filter_by_date_range(input_urja_data, after=start_date, before=end_date)
        expected_result = self.load_expected_results(expected_path)
        actual_result = transformer.urja_to_gridium(input_urja_data)

        self.assertEqual(len(expected_result.periods), len(actual_result.periods))
        expected_sorted = sorted(expected_result.periods, key=lambda p: p.start)
        result_sorted = sorted(actual_result.periods, key=lambda p: p.start)
        for (e, r) in zip(expected_sorted, result_sorted):
            self.assertEqual(e.start, r.start)
            self.assertEqual(e.end, r.end)
            self.assertEqual(e.total_charge, r.total_charge)
            self.assertEqual(e.total_usage, r.total_usage)
            self.assertEqual(e.peak_demand, r.peak_demand)


def default_usage(PK=1,
                  UsageActualName="test_usage_actual_name",
                  UsageAmount=Decimal(100.0),
                  RateComponent="test_rate_component",
                  EnergyUnit="test_energy_unit",
                  IntervalStart=date(2000, 1, 1),
                  IntervalEnd=date(2000, 2, 1)):
    """Create an Urjanet Usage object with all fields filled in"""
    return Usage(
        PK=PK,
        UsageActualName=UsageActualName,
        UsageAmount=UsageAmount,
        RateComponent=RateComponent,
        EnergyUnit=EnergyUnit,
        IntervalStart=IntervalStart,
        IntervalEnd=IntervalEnd)


def default_charge(PK=1,
                   ChargeActualName="test_charge_actual_name",
                   ChargeAmount=Decimal(100.0),
                   UsageUnit="kW",
                   ChargeUnitsUsed=Decimal(20.0),
                   ChargeRatePerUnit=Decimal(5.0),
                   ThirdPartyProvider="test_third_party_provider",
                   IsAdjustmentCharge=False,
                   IntervalStart=date(2000, 1, 1),
                   IntervalEnd=date(2000, 2, 1)):
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
        IntervalEnd=IntervalEnd)


def default_meter(PK=1,
                  Tariff="test_tariff",
                  ServiceType="test_service_type",
                  PODid="test_podid",
                  MeterNumber="test_meter_number",
                  IntervalStart=date(2000, 1, 1),
                  IntervalEnd=date(2000, 2, 1),
                  charges=None,
                  usages=None):
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
        usages=[] if not usages else usages)


# pylint: disable=R0913
def default_account(PK=1,
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
                    floating_charges=None):
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
        floating_charges=[] if not floating_charges else floating_charges)
