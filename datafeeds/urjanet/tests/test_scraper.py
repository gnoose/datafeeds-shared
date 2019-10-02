import os
import unittest
from unittest import mock
from datetime import date

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.scraper import (
    BaseUrjanetScraper,
    BaseUrjanetConfiguration,
    get_charge_kind,
    get_charge_units)
from datafeeds.urjanet.transformer import PacGeGridiumTransfomer

from datafeeds.common.typing import BillingDatum, BillingDatumItemsEntry

TEST_DIR = os.path.split(__file__)[0]
DATA_DIR = os.path.join(TEST_DIR, "data/pacge")


class TestUrjanetScraper(unittest.TestCase):
    def test_get_charge_kind(self):
        """Ensure that the get_charge_kind function works as expected"""
        for usage_unit in ["kwh", "therms", "ccf", "hcf"]:
            charge = test_util.default_charge(UsageUnit=usage_unit)
            self.assertEqual("use", get_charge_kind(charge))
        for demand_unit in ["kw"]:
            charge = test_util.default_charge(UsageUnit=demand_unit)
            self.assertEqual("demand", get_charge_kind(charge))
        for other_unit in ["", "test"]:
            charge = test_util.default_charge(UsageUnit=other_unit)
            self.assertEqual("other", get_charge_kind(charge))

    def test_get_charge_units(self):
        """Ensure that the get_charge_units function works as expected"""
        for usage_unit in ["kwh", "therms", "ccf", "hcf"]:
            charge = test_util.default_charge(UsageUnit=usage_unit)
            self.assertEqual(usage_unit, get_charge_units(charge))
        for demand_unit in ["kw"]:
            charge = test_util.default_charge(UsageUnit=demand_unit)
            self.assertEqual(demand_unit, get_charge_units(charge))
        for other_unit in ["", "test"]:
            charge = test_util.default_charge(UsageUnit=other_unit)
            self.assertEqual("other", get_charge_units(charge))

    def test_basic_scraper_run_no_attachments(self):
        """Ensure that we can run the base Urjanet scraper with simple inputs, without attachments disabled"""
        datasource = test_util.FixtureDataSource(os.path.join(DATA_DIR, "simple_fixture_input.json"))
        transformer = PacGeGridiumTransfomer()
        config = BaseUrjanetConfiguration(datasource, transformer, "pge", False)
        scraper = BaseUrjanetScraper(None, None, config)

        self.assertEqual("Urjanet Scraper: pge", scraper.name)

        result = scraper._execute()

        expected = [
            BillingDatum(
                start=date(2018, 1, 1),
                end=date(2018, 2, 1),
                cost=100.0,
                used=16.0,
                peak=0.0,
                items=[
                    BillingDatumItemsEntry(
                        description='test_charge1',
                        quantity=16.0,
                        rate=5.0,
                        total=80.0,
                        kind='use',
                        unit='kwh'),
                    BillingDatumItemsEntry(
                        description='test_charge2',
                        quantity=0.0,
                        rate=0.0,
                        total=20.0,
                        kind='other',
                        unit='other')],
                attachments=None)]

        self.assertEqual(expected, result.bills)

    # @mock.patch("gridium_tasks.lib.scrapers.urjanet.EtlLogHandler.emit")
    # def test_basic_scraper_logging(self, emit):
    #    """Ensure that the Urjanet logging handler is sent messages"""
    #    datasource = test_util.FixtureDataSource(os.path.join(DATA_DIR, "simple_fixture_input.json"))
    #    transformer = PacGeGridiumTransfomer()
    #    config = BaseUrjanetConfiguration(datasource, transformer, "pge", False, log_level=logging.DEBUG)
    #    etl_logging.create(logger=etl_logging.StdoutETLRunLogger)
    #    scraper = BaseUrjanetScraper(None, None, config)
    #
    #    self.assertEqual("Urjanet Scraper: pge", scraper.name)
    #
    #    scraper._execute()
    #    self.assertTrue(len(emit.call_args_list) > 0)

    # FIXME: Enable Urjanet S3 Upload
    @unittest.skip
    @mock.patch("gridium_tasks.lib.scrapers.urjanet.urjanet_pdf")
    def test_basic_scraper_run_with_attachments(self, mock_urja_pdf):
        """Ensure that we can run the base Urjanet scraper with simple inputs, with attachments enabled"""

        # Mock out the library that uploads to S3, returning 'test_key' as the destination s3 entity
        mock_urja_pdf.statement_to_s3.return_value = "test_key"
        datasource = test_util.FixtureDataSource(os.path.join(DATA_DIR, "simple_fixture_input.json"))
        transformer = PacGeGridiumTransfomer()

        config = BaseUrjanetConfiguration(datasource, transformer, "pge", True)
        scraper = BaseUrjanetScraper(None, None, config)
        result = scraper._execute()

        self.assertEqual(len(result.bills), 1)
        bill = result.bills[0]

        # Note: the attachments are currently represented as a json-encoded list, hence the json.loads
        attachments = bill.attachments
        self.assertEqual(len(attachments), 1)
        attachment = attachments[0]

        # The name of the attachment S3 key should match our mock
        self.assertEqual(attachment.key, "test_key")
        self.assertEqual(attachment.kind, "bill")
        self.assertEqual(attachment.format, "PDF")

    # FIXME: Enable Urjanet S3 Upload
    @unittest.skip
    @mock.patch("gridium_tasks.lib.scrapers.urjanet.urjanet_pdf")
    def test_basic_scraper_run_with_multiple_attachments(self, mock_urja_pdf):
        """Ensure that the Urjanet scraper gracefully handles a bill upload with multiple source links"""

        # Mock out the library that uploads to S3, returning 'test_key' as the destination s3 entity
        mock_urja_pdf.statement_to_s3.return_value = "test_key"
        datasource = test_util.FixtureDataSource(os.path.join(DATA_DIR, "multi_source_link_input.json"))
        transformer = PacGeGridiumTransfomer()

        config = BaseUrjanetConfiguration(datasource, transformer, "pge", True)
        scraper = BaseUrjanetScraper(None, None, config)
        result = scraper._execute()

        self.assertEqual(len(result.bills), 1)
        bill = result.bills[0]

        attachments = bill.attachments
        self.assertEqual(len(attachments), 2)

        for attachment in attachments:
            # The name of the attachment S3 key should match our mock
            self.assertEqual(attachment.key, "test_key")
            self.assertEqual(attachment.kind, "bill")
            self.assertEqual(attachment.format, "PDF")
