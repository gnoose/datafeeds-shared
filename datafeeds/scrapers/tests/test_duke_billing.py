from datetime import date
import logging
import unittest

from datafeeds.scrapers.duke import pdf_parser
from datafeeds.parsers import pdfparser

logging.getLogger("pdfminer").setLevel(logging.WARNING)


class DukeBillTestCase(unittest.TestCase):
    def test_parse_new_pdf1(self):
        """Verify that we can extract cost, use, and demand from June 2020+ version of PDF."""
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_new_1.pdf")
        data = pdf_parser.parse_new_pdf(text)
        self.assertEqual(date(2020, 5, 10), data.start)
        self.assertEqual(date(2020, 6, 9), data.end)
        self.assertAlmostEqual(123113.66, data.cost, 2)
        self.assertAlmostEqual(1806000.0, data.used, 2)
        self.assertAlmostEqual(3840.0, data.peak, 2)

    def test_parse_new_pdf2(self):
        """Verify that we can extract cost, use, and demand from another new-format PDF."""
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_new_2.pdf")
        data = pdf_parser.parse_new_pdf(text)
        self.assertEqual(date(2020, 4, 21), data.start)
        self.assertEqual(date(2020, 5, 25), data.end)
        self.assertAlmostEqual(29.89, data.cost, 2)
        self.assertAlmostEqual(155, data.used, 2)
        self.assertIsNone(data.peak)

    def test_parse_new_pdf3(self):
        """Verify that we can extract cost, use, and demand from another new-format PDF."""
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_new_3.pdf")
        data = pdf_parser.parse_new_pdf(text)
        self.assertEqual(date(2020, 4, 21), data.start)
        self.assertEqual(date(2020, 5, 23), data.end)
        self.assertAlmostEqual(3048.76, data.cost, 2)
        self.assertAlmostEqual(38320, data.used, 2)
        self.assertAlmostEqual(86, data.peak)

    def test_parse_old_pdf1(self):
        """Verify that we can extract cost, use, and demand from version of PDF prior to June 2020.

        This bill includes a prior balance due line item that should be excluded from bill total.
        """
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_old_1.pdf")
        data = pdf_parser.parse_old_pdf(text)
        self.assertEqual(date(2020, 4, 10), data.start)
        self.assertEqual(date(2020, 5, 9), data.end)
        self.assertAlmostEqual(89972.41, data.cost, 2)
        self.assertAlmostEqual(1312000.0, data.used, 2)
        self.assertAlmostEqual(3000.0, data.peak, 2)

    def test_parse_old_pdf2(self):
        """Verify that we can extract cost, use, and demand from another old-format PDF."""
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_old_2.pdf")
        data = pdf_parser.parse_old_pdf(text)
        self.assertEqual(date(2018, 10, 29), data.start)
        self.assertEqual(date(2018, 12, 3), data.end)
        self.assertAlmostEqual(685.16, data.cost, 2)
        self.assertAlmostEqual(5581.0, data.used, 2)
        self.assertAlmostEqual(30.0, data.peak, 2)

    def test_parse_old_pdf3(self):
        """Verify that we can extract cost, use, and demand from another old-format PDF."""
        text = pdfparser.pdf_to_str("private_fixtures/duke_bill_old_3.pdf")
        data = pdf_parser.parse_old_pdf(text)
        self.assertEqual(date(2020, 3, 20), data.start)
        self.assertEqual(date(2020, 4, 20), data.end)
        self.assertAlmostEqual(29.89, data.cost, 2)
        self.assertAlmostEqual(155, data.used, 2)
        self.assertIsNone(data.peak)
