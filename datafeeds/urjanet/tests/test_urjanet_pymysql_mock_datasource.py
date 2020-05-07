import unittest
from datetime import date
from decimal import Decimal

from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
from datafeeds.urjanet.model import UrjanetData, Account, Meter, Usage, Charge


class MockPyMySqlDataSource(UrjanetPyMySqlDataSource):
    """A test datasource containing fake data

    This datasource contains three accounts.
      1) An account with a meter but no floating charges
      2) An account with no meter and no floating charges
      3) An account with floating charges but no meters

    Only accounts (1) and (3) should appear in the loaded datasource, as (2) contributes no data.
    """

    def __init__(self):
        super().__init__(None, "utility:default")

    def load_accounts(self):
        return [
            Account(
                PK=1,
                UtilityProvider="test_provider",
                AccountNumber="acct_1",
                RawAccountNumber="raw_acct_1",
                SourceLink="test_link",
                StatementType="test_stmt_type",
                StatementDate=date(2016, 2, 5),
                IntervalStart=date(2016, 1, 1),
                IntervalEnd=date(2016, 2, 1),
                TotalBillAmount=Decimal(100),
                AmountDue=Decimal(200),
                NewCharges=Decimal(300),
                OutstandingBalance=Decimal(400),
                PreviousBalance=Decimal(500),
                meters=[],
                floating_charges=[],
            ),
            Account(
                PK=2,
                UtilityProvider="test_provider",
                AccountNumber="acct_1",
                RawAccountNumber="raw_acct_1",
                SourceLink="test_link",
                StatementType="test_stmt_type",
                StatementDate=date(2016, 3, 5),
                IntervalStart=date(2016, 2, 1),
                IntervalEnd=date(2016, 3, 1),
                TotalBillAmount=Decimal(101),
                AmountDue=Decimal(201),
                NewCharges=Decimal(301),
                OutstandingBalance=Decimal(401),
                PreviousBalance=Decimal(501),
                meters=[],
                floating_charges=[],
            ),
            Account(
                PK=3,
                UtilityProvider="test_provider",
                AccountNumber="acct_1",
                RawAccountNumber="raw_acct_1",
                SourceLink="test_link",
                StatementType="test_stmt_type",
                StatementDate=date(2016, 4, 5),
                IntervalStart=date(2016, 3, 1),
                IntervalEnd=date(2016, 4, 1),
                TotalBillAmount=Decimal(102),
                AmountDue=Decimal(202),
                NewCharges=Decimal(302),
                OutstandingBalance=Decimal(402),
                PreviousBalance=Decimal(502),
                meters=[],
                floating_charges=[],
            ),
        ]

    def load_meters(self, account_pk):
        meter = Meter(
            PK=4,
            Tariff="test_tariff",
            ServiceType="test_service_type",
            PODid="test_podid",
            MeterNumber="test_meter_number_1",
            IntervalStart=date(2016, 1, 1),
            IntervalEnd=date(2016, 2, 1),
            charges=[],
            usages=[],
        )
        if account_pk == 1:
            return [meter]
        return []

    def load_meter_charges(self, account_pk, meter_pk):
        charge = Charge(
            PK=5,
            ChargeActualName="test_charge",
            ChargeAmount=Decimal(100.0),
            ThirdPartyProvider="test_provider",
            UsageUnit="kW",
            ChargeUnitsUsed=Decimal(25),
            ChargeRatePerUnit=Decimal(4),
            IsAdjustmentCharge=False,
            IntervalStart=date(2016, 1, 1),
            IntervalEnd=date(2016, 2, 1),
        )
        if account_pk == 1 and meter_pk == 4:
            return [charge]
        return []

    def load_meter_usages(self, account_pk, meter_pk):
        usage = Usage(
            PK=6,
            UsageActualName="test_usage",
            UsageAmount=Decimal(50.0),
            RateComponent="test_rate_component",
            EnergyUnit="test_energy_unit",
            IntervalStart=date(2016, 1, 1),
            IntervalEnd=date(2016, 2, 1),
        )
        if account_pk == 1 and meter_pk == 4:
            return [usage]
        return []

    def load_floating_charges(self, account_pk):
        charge = Charge(
            PK=5,
            ChargeActualName="test_charge",
            ChargeAmount=Decimal(200.0),
            ThirdPartyProvider="test_provider",
            UsageUnit="kW",
            ChargeUnitsUsed=Decimal(25),
            ChargeRatePerUnit=Decimal(4),
            IsAdjustmentCharge=False,
            IntervalStart=date(2017, 1, 1),
            IntervalEnd=date(2017, 2, 1),
        )

        if account_pk == 3:
            return [charge]
        return []


class TestUrjanetPyMySqlMockDatasource(unittest.TestCase):
    """Tests ensuring that a mock datasource is correctly loaded"""

    def setUp(self):
        """Load the mock datasource"""
        self.test_data = MockPyMySqlDataSource().load()

    def test_mock_datasource_accounts(self):
        """Ensure that accounts are loaded correctly from the mock datasource"""
        self.assertIsInstance(self.test_data, UrjanetData)
        self.assertIsNotNone(self.test_data.accounts)
        self.assertEqual(len(self.test_data.accounts), 2)

        # This account is loaded because it has a meter
        account1 = self.test_data.accounts[0]
        self.assertIsInstance(account1, Account)
        self.assertEqual(account1.PK, 1)
        self.assertEqual(account1.UtilityProvider, "test_provider")
        self.assertEqual(account1.AccountNumber, "acct_1")
        self.assertEqual(account1.RawAccountNumber, "raw_acct_1")
        self.assertEqual(account1.SourceLink, "test_link")
        self.assertEqual(account1.StatementType, "test_stmt_type")
        self.assertEqual(account1.StatementDate, date(2016, 2, 5))
        self.assertEqual(account1.IntervalStart, date(2016, 1, 1))
        self.assertEqual(account1.IntervalEnd, date(2016, 2, 1))
        self.assertEqual(account1.TotalBillAmount, Decimal(100))
        self.assertEqual(account1.AmountDue, Decimal(200))
        self.assertEqual(account1.NewCharges, Decimal(300))
        self.assertEqual(account1.OutstandingBalance, Decimal(400))
        self.assertEqual(account1.PreviousBalance, Decimal(500))
        self.assertEqual(len(account1.floating_charges), 0)
        self.assertEqual(len(account1.meters), 1)

        # This account is loaded because it has floating charges
        account2 = self.test_data.accounts[1]
        self.assertIsInstance(account2, Account)
        self.assertEqual(account2.PK, 3)
        self.assertEqual(account2.UtilityProvider, "test_provider")
        self.assertEqual(account2.AccountNumber, "acct_1")
        self.assertEqual(account2.RawAccountNumber, "raw_acct_1")
        self.assertEqual(account2.SourceLink, "test_link")
        self.assertEqual(account2.StatementType, "test_stmt_type")
        self.assertEqual(account2.StatementDate, date(2016, 4, 5))
        self.assertEqual(account2.IntervalStart, date(2016, 3, 1))
        self.assertEqual(account2.IntervalEnd, date(2016, 4, 1))
        self.assertEqual(account2.TotalBillAmount, Decimal(102))
        self.assertEqual(account2.AmountDue, Decimal(202))
        self.assertEqual(account2.NewCharges, Decimal(302))
        self.assertEqual(account2.OutstandingBalance, Decimal(402))
        self.assertEqual(account2.PreviousBalance, Decimal(502))
        self.assertEqual(len(account2.floating_charges), 1)
        self.assertEqual(len(account2.meters), 0)

    def test_mock_datasource_meters(self):
        """Ensure that meters are loaded correctly from the mock datasource"""
        account1 = self.test_data.accounts[0]
        meter = account1.meters[0]
        self.assertIsInstance(meter, Meter)
        self.assertEqual(meter.PK, 4)
        self.assertEqual(meter.Tariff, "test_tariff")
        self.assertEqual(meter.ServiceType, "test_service_type")
        self.assertEqual(meter.PODid, "test_podid")
        self.assertEqual(meter.MeterNumber, "test_meter_number_1")
        self.assertEqual(meter.IntervalStart, date(2016, 1, 1))
        self.assertEqual(meter.IntervalEnd, date(2016, 2, 1))
        self.assertEqual(len(meter.charges), 1)
        self.assertEqual(len(meter.usages), 1)

    def test_mock_datasource_charges(self):
        """Ensure that charges are loaded correctly from the mock datasource"""
        account1 = self.test_data.accounts[0]
        meter = account1.meters[0]
        charge = meter.charges[0]
        self.assertIsInstance(charge, Charge)
        self.assertEqual(charge.PK, 5)
        self.assertEqual(charge.ChargeActualName, "test_charge")
        self.assertEqual(charge.ChargeAmount, Decimal(100.0))
        self.assertEqual(charge.UsageUnit, "kW")
        self.assertEqual(charge.ChargeUnitsUsed, Decimal(25))
        self.assertEqual(charge.ChargeRatePerUnit, Decimal(4))
        self.assertEqual(charge.ThirdPartyProvider, "test_provider")
        self.assertEqual(charge.IsAdjustmentCharge, False)
        self.assertEqual(charge.IntervalStart, date(2016, 1, 1))
        self.assertEqual(charge.IntervalEnd, date(2016, 2, 1))

    def test_mock_datasource_usages(self):
        """Ensure that usages are loaded correctly from the mock datasource"""
        account1 = self.test_data.accounts[0]
        meter = account1.meters[0]
        usage = meter.usages[0]
        self.assertIsInstance(usage, Usage)
        self.assertEqual(usage.PK, 6)
        self.assertEqual(usage.UsageActualName, "test_usage")
        self.assertEqual(usage.UsageAmount, Decimal(50.0))
        self.assertEqual(usage.RateComponent, "test_rate_component")
        self.assertEqual(usage.EnergyUnit, "test_energy_unit")
        self.assertEqual(usage.IntervalStart, date(2016, 1, 1))
        self.assertEqual(usage.IntervalEnd, date(2016, 2, 1))

    def test_mock_datasource_floating_charges(self):
        """Ensure that floating charges are loaded correctly from the mock datasource"""
        account2 = self.test_data.accounts[1]
        floating_charge = account2.floating_charges[0]
        self.assertIsInstance(floating_charge, Charge)
        self.assertEqual(floating_charge.PK, 5)
        self.assertEqual(floating_charge.ChargeActualName, "test_charge")
        self.assertEqual(floating_charge.ChargeAmount, Decimal(200.0))
        self.assertEqual(floating_charge.UsageUnit, "kW")
        self.assertEqual(floating_charge.ChargeUnitsUsed, Decimal(25))
        self.assertEqual(floating_charge.ChargeRatePerUnit, Decimal(4))
        self.assertEqual(floating_charge.ThirdPartyProvider, "test_provider")
        self.assertEqual(floating_charge.IsAdjustmentCharge, False)
        self.assertEqual(floating_charge.IntervalStart, date(2017, 1, 1))
        self.assertEqual(floating_charge.IntervalEnd, date(2017, 2, 1))


if __name__ == "__main__":
    unittest.main()
