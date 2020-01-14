import unittest
from unittest.mock import patch, ANY

import datafeeds.scrapers.sdge_myaccount
from datafeeds import db
from datafeeds.common import test_utils
from datafeeds.common.exceptions import DataSourceConfigurationError, LoginError
from datafeeds.models.account import SnapmeterAccount
from datafeeds.models.datasource import SnapmeterMeterDataSource
from datafeeds.models.meter import Meter


class SDGEMyAccountTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    def setUp(self):
        (account, meters) = test_utils.create_meters()
        test_utils.add_datasources(account, meters, "sdge_myaccount")
        self.account_oid = account.oid
        self.meter_ids = [m.oid for m in meters]

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    @patch("datafeeds.common.alert.post_slack_message")
    def test_skip_disabled(self, slack):
        """Verify that a disabled datasource does not run."""
        meter_id = self.meter_ids[0]
        mds = (
            db.session.query(SnapmeterMeterDataSource).filter_by(_meter=meter_id).one()
        )
        account = db.session.query(SnapmeterAccount).get(self.account_oid)
        meter = db.session.query(Meter).get(meter_id)
        mds.utility_account_id = str(meter_id)
        db.session.add(mds)
        params = {}
        # disable data source
        account_ds = mds.account_data_source
        account_ds.enabled = False
        db.session.add(account_ds)
        db.session.flush()
        self.assertRaises(
            DataSourceConfigurationError,
            datafeeds.scrapers.sdge_myaccount.datafeed,
            account,
            meter,
            mds,
            params,
        )
        slack.assert_not_called()

    @patch("datafeeds.common.batch.log")
    @patch("datafeeds.common.base.BaseWebScraper.start")
    @patch("datafeeds.common.base.BaseWebScraper.stop")
    @patch("datafeeds.common.alert.post_slack_message")
    @patch("datafeeds.scrapers.sdge_myaccount.SdgeMyAccountScraper.scrape")
    def test_login_error(self, scrape, slack, _stop, _start, _log):
        """Verify that a LoginException disables related data sources."""
        meter_id = self.meter_ids[0]
        mds = (
            db.session.query(SnapmeterMeterDataSource).filter_by(_meter=meter_id).one()
        )
        account = db.session.query(SnapmeterAccount).get(self.account_oid)
        meter = db.session.query(Meter).get(meter_id)
        mds.utility_account_id = str(meter_id)
        db.session.add(mds)
        params = {}

        # meter data source not disabled: call run_datafeed
        datafeeds.scrapers.sdge_myaccount.datafeed(account, meter, mds, params)
        self.assertEqual(1, scrape.call_count, "called scrape")
        slack.assert_not_called()
        for mid in self.meter_ids:
            mds = db.session.query(SnapmeterMeterDataSource).filter_by(_meter=mid).one()
            self.assertEqual("abc", mds.meta["test"], "meta.test still set")
            self.assertFalse(mds.meta.get("disabled"), "meta.disabled unset")

        # LoginError disables
        account_ds = mds.account_data_source
        account_ds.enabled = True
        db.session.add(account_ds)
        db.session.flush()
        scrape.side_effect = LoginError()
        datafeeds.scrapers.sdge_myaccount.datafeed(account, meter, mds, params)
        msg = slack.call_args_list[0][0][0]
        self.assertTrue(account.name in msg)
        for meter_id in self.meter_ids:
            self.assertTrue(db.session.query(Meter).get(meter_id).name in msg)
        slack.called_once_with(
            ANY, "#scrapers", ":exclamation:", username="Scraper monitor"
        )
        # account data source disabled
        db.session.flush()
        db.session.refresh(account_ds)
        self.assertFalse(account_ds.enabled)
