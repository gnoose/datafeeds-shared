import unittest
from unittest.mock import patch, ANY

from datafeeds import db
from datafeeds.common import test_utils
from datafeeds.common.exceptions import DataSourceConfigurationError, LoginError
from datafeeds.datasources import austin_energy_interval, pacific_power_interval, smud_interval
from datafeeds.models.account import SnapmeterAccount
from datafeeds.models.datasource import SnapmeterMeterDataSource
from datafeeds.models.meter import Meter


# these all use the same epo_schneider scraper
DATA_SOURCES = {
    "austin-energy-interval": austin_energy_interval,
    "pacific-power-interval": pacific_power_interval,
    "smud-interval": smud_interval,
}


class EPOSchneiderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        test_utils.init_test_db()

    def setUp(self):
        (account, meters) = test_utils.create_meters()
        test_utils.add_datasources(account, meters, "austin-energy-interval")
        self.account_oid = account.oid
        self.meter_ids = [m.oid for m in meters]

    @classmethod
    def tearDown(cls):
        db.session.rollback()

    @patch("datafeeds.common.alert.post_slack_message")
    def test_skip_disabled(self, slack):
        """Verify that a disabled datasource does not run."""
        meter_id = self.meter_ids[0]
        mds = db.session.query(SnapmeterMeterDataSource).filter_by(_meter=meter_id).one()
        account = db.session.query(SnapmeterAccount).get(self.account_oid)
        meter = db.session.query(Meter).get(meter_id)
        mds.utility_account_id = str(meter_id)
        db.session.add(mds)
        params = {}
        # disable data source
        account_ds = mds.account_data_source
        for ds_name in DATA_SOURCES:
            account_ds.enabled = False
            account_ds.source_account_type = ds_name
            db.session.add(account_ds)
            db.session.flush()
            self.assertRaises(
               DataSourceConfigurationError,
               DATA_SOURCES[ds_name].datafeed, account, meter, mds, params)
            slack.assert_not_called()

    @patch("datafeeds.common.alert.post_slack_message")
    def test_login_error(self, slack):
        """Verify that a LoginException disables related data sources."""
        meter_id = self.meter_ids[0]
        mds = db.session.query(SnapmeterMeterDataSource).filter_by(_meter=meter_id).one()
        account = db.session.query(SnapmeterAccount).get(self.account_oid)
        meter = db.session.query(Meter).get(meter_id)
        mds.utility_account_id = str(meter_id)
        db.session.add(mds)
        params = {}
        for ds_name in DATA_SOURCES:
            with patch("datafeeds.datasources.%s.run_datafeed" % ds_name.replace("-", "_")) as run:
                # meter data source not disabled: call run_datafeed
                DATA_SOURCES[ds_name].datafeed(account, meter, mds, params)
                self.assertEqual(1, run.call_count)
                slack.assert_not_called()
                run.reset_mock()
                for mid in self.meter_ids:
                    mds = db.session.query(SnapmeterMeterDataSource).filter_by(_meter=mid).one()
                    self.assertEqual("abc", mds.meta["test"], "meta.test still set")
                    self.assertFalse(mds.meta.get("disabled"), "meta.disabled unset")

                # LoginException disables
                account_ds = mds.account_data_source
                account_ds.enabled = True
                account_ds.source_account_type = ds_name
                db.session.add(account_ds)
                db.session.flush()
                run.side_effect = LoginError()
                self.assertRaises(
                    LoginError,
                    DATA_SOURCES[ds_name].datafeed, account, meter, mds, params)
                msg = slack.call_args_list[0][0][0]
                self.assertTrue(account.name in msg)
                for meter_id in self.meter_ids:
                    self.assertTrue(db.session.query(Meter).get(meter_id).name in msg)
                slack.called_once_with(
                    ANY, "#scrapers", ":exclamation:", username="Scraper monitor")
                slack.reset_mock()
                # account data source disabled
                db.session.flush()
                db.session.refresh(account_ds)
                self.assertFalse(account_ds.enabled)
