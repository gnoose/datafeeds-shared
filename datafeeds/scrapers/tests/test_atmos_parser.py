from datetime import date
import os
from unittest import TestCase

from datafeeds.common import BillingDatum
from datafeeds.scrapers.atmos.parsers import bill_data_from_pdf, bill_data_from_xls

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


# Note: Expected values contain divisions by 1.036 so that we can easily cross-reference the
# use values (in CCF) listed in excel/pdf bills.


class TestAtmosParser(TestCase):
    def setUp(self) -> None:
        files = [
            "atmos-example-01.pdf",
            "atmos-example-02.pdf",
            "atmos-example-03.pdf",
            "atmos-example-04.pdf",
            "atmos-example-05.pdf",
            "atmos-example-06.pdf",
            "atmos-example-07.pdf",
            "atmos-example-08.pdf",
            "atmos-example-01.xls",
            "atmos-example-02.xls",
            "atmos-example-03.xls",
        ]
        self.data = {}
        for filename in files:
            with open(os.path.join(TEST_DIR, "fixtures", filename), "rb") as f:
                self.data[filename] = f.read()

    def test_bill_data_from_pdf_0(self):
        """If the target service account is not present in the PDF, the parser returns None."""
        actual = bill_data_from_pdf(
            self.data["atmos-example-01.pdf"], "123456", "123456"
        )
        self.assertEqual([], actual)

    def test_bill_data_from_pdf_1(self):
        service_account = "4018250552"
        meter_serial = "16E912776C"
        actual = bill_data_from_pdf(
            self.data["atmos-example-01.pdf"], service_account, meter_serial
        )
        expected = [
            BillingDatum(
                start=date(2019, 5, 31),
                end=date(2019, 6, 26),
                cost=53.12,
                used=6.00 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            )
        ]
        self.assertEqual(expected, actual)

    def test_bill_data_from_pdf_2(self):
        service_account = "3024771103"
        meter_serial = "000226033C"
        actual = bill_data_from_pdf(
            self.data["atmos-example-02.pdf"], service_account, meter_serial
        )
        expected = [
            BillingDatum(
                start=date(2019, 4, 16),
                end=date(2019, 5, 14),
                cost=173.82,
                used=327.00 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            )
        ]
        self.assertEqual(expected, actual)

    def test_bill_data_from_pdf_3(self):
        service_account = "3049062529"
        meter_serial = "10L680441"
        actual = bill_data_from_pdf(
            self.data["atmos-example-03.pdf"], service_account, meter_serial
        )
        expected = [
            BillingDatum(
                start=date(2019, 6, 7),
                end=date(2019, 7, 5),
                cost=21.15,
                used=9.51 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            )
        ]
        self.assertEqual(expected, actual)

    def test_bill_data_from_pdf_4(self):
        service_account = "3028521652"
        meter_serial = "044003532C"
        actual = bill_data_from_pdf(
            self.data["atmos-example-04.pdf"], service_account, meter_serial
        )
        expected = [
            BillingDatum(
                start=date(2017, 10, 12),
                end=date(2017, 11, 13),
                cost=328.14,
                used=407.00 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            )
        ]
        self.assertEqual(expected, actual)

    def test_bill_data_from_pdf_5(self):
        service_account = "3028521652"
        meter_serial = "044003532C"
        actual = bill_data_from_pdf(
            self.data["atmos-example-05.pdf"], service_account, meter_serial
        )
        expected = [
            BillingDatum(
                start=date(2017, 12, 13),
                end=date(2018, 1, 10),
                cost=222.30,
                used=264.00 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            )
        ]
        self.assertEqual(expected, actual)

    def test_bill_data_from_pdf_6(self):
        service_account = "3028521652"
        meter_serial = "044003532C"
        actual = bill_data_from_pdf(
            self.data["atmos-example-06.pdf"], service_account, meter_serial
        )
        expected = [
            BillingDatum(
                start=date(2017, 11, 14),
                end=date(2017, 12, 12),
                cost=307.54,
                used=345.00 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            )
        ]
        self.assertEqual(expected, actual)

    def test_bill_data_from_pdf_7(self):
        service_account = "3024769590"
        meter_serial = "000214831C"
        actual = bill_data_from_pdf(
            self.data["atmos-example-07.pdf"], service_account, meter_serial
        )
        expected = [
            BillingDatum(
                start=date(2018, 6, 27),
                end=date(2018, 7, 26),
                cost=51.24,
                used=10.0 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            )
        ]
        self.assertEqual(expected, actual)

    def test_bill_data_from_pdf_8(self):
        service_account = "3024769590"
        meter_serial = "000214831C"
        actual = bill_data_from_pdf(
            self.data["atmos-example-08.pdf"], service_account, meter_serial
        )
        expected = [
            BillingDatum(
                start=date(2018, 3, 24),
                end=date(2018, 4, 23),
                cost=522.13,
                used=750.0 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            ),
            BillingDatum(
                start=date(2018, 4, 24),
                end=date(2018, 5, 24),
                cost=745.26,
                used=1104.0 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            ),
        ]
        self.assertEqual(expected, actual)

    def test_bill_data_from_xls_0(self):
        """If the target service account is not present in the spreadsheet, the parser returns None."""
        actual = bill_data_from_xls(self.data["atmos-example-01.xls"], "123456")
        self.assertEqual([], actual)

    def test_bill_data_from_xls_1(self):
        service_account = "4018250552"
        actual = bill_data_from_xls(self.data["atmos-example-01.xls"], service_account)
        expected = [
            BillingDatum(
                start=date(2019, 5, 31),
                end=date(2019, 6, 26),
                cost=53.12,
                used=6.00 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            )
        ]
        self.assertEqual(expected, actual)

    def test_bill_data_from_xls_2(self):
        service_account = "4023761535"

        actual = bill_data_from_xls(self.data["atmos-example-02.xls"], service_account)
        expected = [
            BillingDatum(
                start=date(2018, 5, 23),
                end=date(2018, 6, 26),
                cost=54.14,
                used=8.0 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            )
        ]
        self.assertEqual(expected, actual)

    def test_bill_data_from_xls_3(self):
        service_account = "3024769590"

        actual = bill_data_from_xls(self.data["atmos-example-03.xls"], service_account)
        expected = [
            BillingDatum(
                start=date(2018, 6, 27),
                end=date(2018, 7, 26),
                cost=51.24,
                used=10.0 * 1.036,
                peak=None,
                items=None,
                attachments=None,
            )
        ]
        self.assertEqual(expected, actual)
