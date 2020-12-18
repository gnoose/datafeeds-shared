import logging
import unittest

from datafeeds.parsers import pdfparser
from datafeeds.scrapers.svp import pdf_parser

logging.getLogger("pdfminer").setLevel(logging.WARNING)


class SetupBase(unittest.TestCase):
    def test_power_factor_charge(self):
        """The parser handles a bill with a Power Factor Charge line"""
        text = pdfparser.pdf_to_str("private_fixtures/svp_bill_1.pdf")
        data = pdf_parser.extract_line_items(text)
        self.assertEqual(data[3][3], -99.05)

    def test_no_power_factor_charge(self):
        """The parser handles a bill without a Power Factor Charge line"""
        text = pdfparser.pdf_to_str("private_fixtures/svp_bill_2.pdf")
        data = pdf_parser.extract_line_items(text)
        self.assertEqual(data[3][3], None)
