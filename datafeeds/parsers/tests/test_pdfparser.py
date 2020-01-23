import unittest
import os

from datafeeds.parsers import pdfparser


TEST_DIR = os.path.split(__file__)[0]


class TestPdfParser(unittest.TestCase):
    def test_pdf_to_str(self):
        output = pdfparser.pdf_to_str(
            os.path.join(TEST_DIR, "fixtures", "test_portland_bizportal_bill.pdf")
        )
        self.assertEqual(type(output), str)
        self.assertGreater(
            len(output), 100
        )  # The string for this pdf should be quite long.
        self.assertIn("Service Period", output)  # Some text from this PDF
