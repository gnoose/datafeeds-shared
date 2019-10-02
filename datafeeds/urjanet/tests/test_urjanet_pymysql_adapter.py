# pylint: disable=R0904

import unittest
from datetime import date
from decimal import Decimal

from datafeeds.urjanet.datasource.pymysql_adapter import (
    get_column, get_bool, get_date, get_int, get_str, get_decimal,
    UrjanetPyMySqlDataSource)


class TestUrjanetPyMySqlAdapter(unittest.TestCase):
    """Test the various functions used to extract values from a PyMySQL query"""

    def test_get_column_basic(self):
        """Test the basic functionality of the get_column function"""
        row = {"col1": "val1", "col2": 2, "col3": True}
        self.assertEqual(get_column(row, "col1"), "val1")
        self.assertEqual(get_column(row, "col2"), 2)
        self.assertEqual(get_column(row, "col3"), True)

    def test_get_column_transform(self):
        """Ensure that the transform argument to get_column works as expected"""
        row = {"col1": 1, "col2": 2, "col3": 3}
        self.assertEqual(get_column(row, "col1", transform=str), "1")
        self.assertEqual(get_column(row, "col2", transform=lambda x: x + 1), 3)
        self.assertEqual(get_column(row, "col3", transform=lambda x: 0), 0)

    def test_get_column_nullable(self):
        """Ensure that the nullable argument to get_column works as expected"""
        row = {"col1": None}
        self.assertEqual(get_column(row, "col1"), None)
        self.assertEqual(get_column(row, "col1", nullable=True), None)
        with self.assertRaises(ValueError):
            get_column(row, "col1", nullable=False)

    def test_get_column_transform_valuerror(self):
        """Ensure that a ValueError is raised when transformation fails in get_column"""
        row = {"col1": "not_an_int"}
        with self.assertRaises(ValueError):
            get_column(row, "col1", transform=int)

    def test_get_column_enforce_type(self):
        """Ensure that the enforce_type argument to get_column works as expected"""
        row = {"col1": 1, "col2": 2}
        self.assertEqual(get_column(row, "col1", enforce_type=int), 1)
        self.assertEqual(get_column(row, "col2", enforce_type=int), 2)

    def test_get_column_enforce_type_typeerror(self):
        """Ensure that a TypeError is raised when type enforcement fails in get_column"""
        row = {"col1": 1, "col2": 2}
        with self.assertRaises(TypeError):
            get_column(row, "col1", enforce_type=str)
        with self.assertRaises(TypeError):
            get_column(row, "col2", enforce_type=float)

    def test_get_column_keyerror(self):
        """Ensure that a KeyError is raised when a column name isn't valid in get_column"""
        row = {"col1": 1, "col2": 2, "col3": 3}
        with self.assertRaises(KeyError):
            get_column(row, "col_not_here")

    def test_get_column_transform_and_enforce_type(self):
        """Test the transform and enforce_type arguments together in get_column"""
        row = {"col1": "1", "col2": 2}

        self.assertEqual(
            get_column(row, "col1", transform=int, enforce_type=int), 1)

        with self.assertRaises(TypeError):
            get_column(row, "col1", transform=str, enforce_type=int)

    def test_get_int(self):
        """Test the basic functionality of the get_int function"""
        row = {"col1": 1, "col2": 2}
        self.assertEqual(get_int(row, "col1"), 1)
        self.assertEqual(get_int(row, "col2"), 2)

    def test_get_int_nullable(self):
        """Ensure that the nullable argument to get_int works as expected"""
        row = {"col1": None}
        self.assertEqual(get_int(row, "col1"), None)
        self.assertEqual(get_int(row, "col1", nullable=True), None)
        with self.assertRaises(ValueError):
            get_int(row, "col1", nullable=False)

    def test_get_int_from_string(self):
        """Test get_int with a string input"""
        row = {"col1": "1"}
        self.assertEqual(get_int(row, "col1"), 1)

    def test_get_int_with_value_error(self):
        """Test that get_int raises a ValueError when passed an invalid value"""
        row = {"col": "one"}
        with self.assertRaises(ValueError):
            get_int(row, "col")

    def test_get_int_with_type_error(self):
        """Test that get_int raises a TypeError when transform produces the wrong type"""
        row = {"col": 1}
        with self.assertRaises(TypeError):
            get_int(row, "col", transform=str)

    def test_get_bool(self):
        """Test the basic functionality of the get_bool function"""
        row = {"col1": True, "col2": False}
        self.assertEqual(get_bool(row, "col1"), True)
        self.assertEqual(get_bool(row, "col2"), False)

    def test_get_bool_from_int(self):
        """Test get_int with an int input"""
        row = {"col1": 1, "col2": 0}
        self.assertEqual(get_bool(row, "col1"), True)
        self.assertEqual(get_bool(row, "col2"), False)

    def test_get_bool_nullable(self):
        """Ensure that the nullable argument to get_bool works as expected"""
        row = {"col1": None}
        self.assertEqual(get_bool(row, "col1"), None)
        self.assertEqual(get_bool(row, "col1", nullable=True), None)
        with self.assertRaises(ValueError):
            get_bool(row, "col1", nullable=False)

    def test_get_str(self):
        """Test the basic functionality of the get_str function"""
        row = {"col1": "foo", "col2": 1, "col3": True}
        self.assertEqual(get_str(row, "col1"), "foo")
        self.assertEqual(get_str(row, "col2"), "1")
        self.assertEqual(get_str(row, "col3"), "True")

    def test_get_str_nullable(self):
        """Ensure that the nullable argument to get_str works as expected"""
        row = {"col1": None}
        self.assertEqual(get_str(row, "col1"), None)
        self.assertEqual(get_str(row, "col1", nullable=True), None)
        with self.assertRaises(ValueError):
            get_str(row, "col1", nullable=False)

    def test_get_date(self):
        """Test the basic functionality of the get_date function"""
        row = {"col1": date(2000, 1, 1)}
        self.assertEqual(get_date(row, "col1"), date(2000, 1, 1))

    def test_get_date_with_value_error(self):
        """Test that get_date raises a ValueError when passed an invalid value"""
        row = {"col1": "not_a_date"}
        with self.assertRaises(ValueError):
            get_date(row, "col1")

    def test_get_date_nullable(self):
        """Ensure that the nullable argument to get_date works as expected"""
        row = {"col1": None}
        self.assertEqual(get_date(row, "col1"), None)
        self.assertEqual(get_date(row, "col1", nullable=True), None)
        with self.assertRaises(ValueError):
            get_date(row, "col1", nullable=False)

    def test_get_decimal(self):
        """Test the basic functionality of the get_decimal function"""
        row = {"col1": Decimal('100.00')}
        self.assertEqual(get_decimal(row, "col1"), Decimal('100.00'))

    def test_get_decimal_nullable(self):
        """Ensure that the nullable argument to get_decimal works as expected"""
        row = {"col1": None}
        self.assertEqual(get_decimal(row, "col1"), None)
        self.assertEqual(get_decimal(row, "col1", nullable=True), None)
        with self.assertRaises(ValueError):
            get_decimal(row, "col1", nullable=False)

    def test_parse_account_row(self):
        """Test the basic functionality of the parse_account_row function."""
        account_row = {
            "PK": 1,
            "UtilityProvider": "test_provider",
            "AccountNumber": "12345",
            "RawAccountNumber": "1234-5",
            "SourceLink": "test_link",
            "StatementType": "test_statement_type",
            "StatementDate": date(2000, 1, 1),
            "IntervalStart": date(2000, 2, 1),
            "IntervalEnd": date(2000, 3, 1),
            "TotalBillAmount": Decimal("100.00"),
            "AmountDue": Decimal("200.00"),
            "NewCharges": Decimal("80.00"),
            "OutstandingBalance": Decimal("90.00"),
            "PreviousBalance": Decimal("120.00"),
            "__EXTRA1": "EXTRA1",  # It's okay to have extra fields
            "__EXTRA2": "EXTRA2"
        }
        result = UrjanetPyMySqlDataSource.parse_account_row(account_row)
        for field in account_row:
            if field.startswith("__EXTRA"):
                with self.assertRaises(AttributeError):
                    getattr(result, field)
            else:
                self.assertEqual(getattr(result, field), account_row[field])

    def test_parse_account_row_valueerror(self):
        """Test that parse_account_row raises a ValueError when a field has an invalid value"""
        account_row = {
            "PK": 1,
            "UtilityProvider": "test_provider",
            "AccountNumber": "12345",
            "RawAccountNumber": "1234-5",
            "SourceLink": "test_link",
            "StatementType": "test_statement_type",
            "StatementDate": "not_a_date",  # ValueError should occur here
            "IntervalStart": date(2000, 2, 1),
            "IntervalEnd": date(2000, 3, 1),
            "TotalBillAmount": Decimal("100.00"),
            "AmountDue": Decimal("200.00"),
            "NewCharges": Decimal("80.00"),
            "OutstandingBalance": Decimal("90.00"),
            "PreviousBalance": Decimal("120.00")
        }
        with self.assertRaises(ValueError):
            UrjanetPyMySqlDataSource.parse_account_row(account_row)

    def test_parse_meter_row(self):
        """Test the basic functionality of the parse_meter_row function."""
        meter_row = {
            "PK": 1,
            "Tariff": "test_tariff",
            "ServiceType": "test_service",
            "PODid": "12345",
            "MeterNumber": "67890",
            "IntervalStart": date(2000, 2, 1),
            "IntervalEnd": date(2000, 3, 1),
            "__EXTRA1": "EXTRA1",  # It's okay to have extra fields
            "__EXTRA2": "EXTRA2"
        }
        result = UrjanetPyMySqlDataSource.parse_meter_row(meter_row)
        for field in meter_row:
            if field.startswith("__EXTRA"):
                with self.assertRaises(AttributeError):
                    getattr(result, field)
            else:
                self.assertEqual(getattr(result, field), meter_row[field])

    def test_parse_meter_row_valueerror(self):
        """Test that parse_meter_row raises a ValueError when a field has an invalid value"""
        meter_row = {
            "PK": "not_an_int",  # ValueError occurs here
            "Tariff": "test_tariff",
            "ServiceType": "test_service",
            "PODid": "12345",
            "MeterNumber": "67890",
            "IntervalStart": date(2000, 2, 1),
            "IntervalEnd": date(2000, 3, 1)
        }
        with self.assertRaises(ValueError):
            UrjanetPyMySqlDataSource.parse_meter_row(meter_row)

    def test_parse_meter_row_keyerror(self):
        """Test that parse_meter_row raises a KeyError when a field is missing"""
        meter_row = {
            "PK": 1,
            "Tariff": "test_tariff",
            # Exclude this field"ServiceType": "test_service",
            "PODid": "12345",
            "MeterNumber": "67890",
            "IntervalStart": date(2000, 2, 1),
            "IntervalEnd": date(2000, 3, 1)
        }
        with self.assertRaises(KeyError):
            UrjanetPyMySqlDataSource.parse_meter_row(meter_row)

    def test_parse_charge_row(self):
        """Test the basic functionality of the parse_charge_row function."""
        charge_row = {
            "PK": 1,
            "ChargeActualName": "test_charge_name",
            "ChargeAmount": Decimal(100),
            "UsageUnit": "kW",
            "ChargeUnitsUsed": Decimal(200),
            "ChargeRatePerUnit": Decimal(10),
            "ThirdPartyProvider": "test_provider",
            "IsAdjustmentCharge": 0,
            "IntervalStart": date(2000, 2, 1),
            "IntervalEnd": date(2000, 3, 1),
            "__EXTRA1": "EXTRA1",  # It's okay to have extra fields
            "__EXTRA2": "EXTRA2"
        }
        result = UrjanetPyMySqlDataSource.parse_charge_row(charge_row)
        for field in charge_row:
            if field.startswith("__EXTRA"):
                with self.assertRaises(AttributeError):
                    getattr(result, field)
            else:
                self.assertEqual(getattr(result, field), charge_row[field])

    def test_parse_charge_row_no_provider(self):
        """Ensure that parse_charge_row works when ThirdPartyProvider is None"""
        charge_row = {
            "PK": 1,
            "ChargeActualName": "test_charge_name",
            "ChargeAmount": Decimal(100.00),
            "UsageUnit": "kW",
            "ChargeUnitsUsed": Decimal(200),
            "ChargeRatePerUnit": Decimal(10),
            "ThirdPartyProvider": None,
            "IsAdjustmentCharge": 0,
            "IntervalStart": date(2000, 2, 1),
            "IntervalEnd": date(2000, 3, 1)
        }
        result = UrjanetPyMySqlDataSource.parse_charge_row(charge_row)
        for field in charge_row:
            self.assertEqual(getattr(result, field), charge_row[field])

    def test_parse_charge_row_valueerror(self):
        """Test that parse_charge_row raises a ValueError when a field has an invalid value"""
        charge_row = {
            "PK": 1,
            "ChargeActualName": "test_charge_name",
            "ChargeAmount": "not_a_decimal",  # ValueError here
            "UsageUnit": "kW",
            "ChargeUnitsUsed": Decimal(200),
            "ChargeRatePerUnit": Decimal(10),
            "ThirdPartyProvider": "test_provider",
            "IsAdjustmentCharge": 0,
            "IntervalStart": date(2000, 2, 1),
            "IntervalEnd": date(2000, 3, 1)
        }
        with self.assertRaises(ValueError):
            UrjanetPyMySqlDataSource.parse_charge_row(charge_row)

    def test_parse_charge_row_nil_pk(self):
        """Test that parse_charge_row raises a ValueError when the primary key field is missing"""
        charge_row = {
            "PK": None,
            "ChargeActualName": "test_charge_name",
            "ChargeAmount": Decimal(100.00),
            "UsageUnit": "kW",
            "ChargeUnitsUsed": Decimal(200),
            "ChargeRatePerUnit": Decimal(10),
            "ThirdPartyProvider": "test_provider",
            "IsAdjustmentCharge": 0,
            "IntervalStart": date(2000, 2, 1),
            "IntervalEnd": date(2000, 3, 1)
        }
        with self.assertRaises(ValueError):
            UrjanetPyMySqlDataSource.parse_charge_row(charge_row)

    def test_parse_charge_row_keyerror(self):
        """Test that parse_charge_row raises a KeyError when a field is missing"""
        charge_row = {
            "PK": 1,
            "ChargeActualName": "test_charge_name",
            "UsageUnit": "kW",
            "ChargeUnitsUsed": Decimal(200),
            "ChargeRatePerUnit": Decimal(10),
            # Exclude this field: "ChargeAmount": Decimal(100.00)
            "ThirdPartyProvider": "test_provider",
            "IntervalStart": date(2000, 2, 1),
            "IntervalEnd": date(2000, 3, 1)
        }
        with self.assertRaises(KeyError):
            UrjanetPyMySqlDataSource.parse_charge_row(charge_row)

    def test_parse_usage_row(self):
        """Test the basic functionality of the parse_usage_row function."""
        usage_row = {
            "PK": 1,
            "UsageActualName": "test_charge_name",
            "UsageAmount": Decimal(100.00),
            "RateComponent": "test_component",
            "EnergyUnit": "test_unit",
            "IntervalStart": date(2000, 2, 1),
            "IntervalEnd": date(2000, 3, 1),
            "__EXTRA1": "EXTRA1",  # It's okay to have extra fields
            "__EXTRA2": "EXTRA2"
        }
        result = UrjanetPyMySqlDataSource.parse_usage_row(usage_row)
        for field in usage_row:
            if field.startswith("__EXTRA"):
                with self.assertRaises(AttributeError):
                    getattr(result, field)
            else:
                self.assertEqual(getattr(result, field), usage_row[field])


if __name__ == "__main__":
    unittest.main()
