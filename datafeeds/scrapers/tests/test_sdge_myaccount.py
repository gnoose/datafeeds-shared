import unittest
from collections import defaultdict
from unittest.mock import patch, ANY
from datetime import datetime, date

import datafeeds.scrapers.sdge_myaccount
from datafeeds import db
from datafeeds.common import test_utils
from datafeeds.common.exceptions import DataSourceConfigurationError, LoginError
from datafeeds.common.timeline import Timeline
from datafeeds.models.account import SnapmeterAccount
from datafeeds.models.datasource import SnapmeterMeterDataSource
from datafeeds.scrapers.sdge_myaccount import (
    adjust_for_dst,
    extract_csv_rows,
    to_raw_reading,
    parse_xlsx,
)
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

    def test_adjust_for_dst(self):
        """Test daylight savings time interval adjustments
        First day of DST (in spring), zeroes are turned to None, 2AM - 3AM
        Last day of DST (in fall), interval values are halved, 1AM - 2AM
        Otherwise, readings returned unchanged.
        """

        readings = [
            9.6,
            10.24,
            9.6,
            9.6,
            9.6,
            10.24,
            9.6,
            10.24,
            0.0,
            0.0,
            0.0,
            0.0,
            10.24,
            9.6,
            10.24,
            9.6,
            9.6,
            10.24,
            9.6,
            10.24,
            9.6,
            9.6,
            9.6,
            10.24,
            9.6,
            9.6,
            10.24,
            9.6,
            9.6,
            9.6,
            9.6,
            8.32,
            8.32,
            8.32,
            8.32,
            8.32,
            7.68,
            8.32,
            8.32,
            8.32,
            10.88,
            8.32,
            8.96,
            7.68,
            8.32,
            8.32,
            8.32,
            8.96,
            8.32,
            7.68,
            8.96,
            7.68,
            8.32,
            8.32,
            8.32,
            8.32,
            8.32,
            8.32,
            8.32,
            8.32,
            8.32,
            8.32,
            8.32,
            8.32,
            7.68,
            8.96,
            8.32,
            8.32,
            8.32,
            7.68,
            8.96,
            8.32,
            9.6,
            9.6,
            10.24,
            9.6,
            9.6,
            10.24,
            9.6,
            9.6,
            9.6,
            9.6,
            10.24,
            9.6,
            12.8,
            10.24,
            9.6,
            9.6,
            9.6,
            10.24,
            9.6,
            10.24,
            9.6,
            9.6,
            10.24,
            9.6,
        ]

        date_fmt = "%m/%d/%Y"

        spring_dst = datetime.strptime("3/8/2020", date_fmt).date()
        spring_dst_readings = adjust_for_dst(spring_dst, readings.copy())
        self.assertEqual([None, None, None, None, 10.24], spring_dst_readings[8:13])

        summer_day = datetime.strptime("7/20/2020", date_fmt).date()
        summer_dst_readings = adjust_for_dst(summer_day, readings.copy())
        self.assertEqual(readings[0:13], summer_dst_readings[0:13])

        fall_dst = datetime.strptime("11/1/2020", date_fmt).date()
        fall_dst_readings = adjust_for_dst(fall_dst, readings.copy())
        self.assertEqual(
            [4.8, 5.12, 4.8, 5.12, 0.0, 0.0, 0.0, 0.0, 10.24], fall_dst_readings[4:13]
        )


class SDGECSVParsingTests(unittest.TestCase):
    def test_parse_15_min_electric(self):
        raw_readings = defaultdict(list)
        for row in extract_csv_rows(
            "datafeeds/scrapers/tests/fixtures/sdge_15_min_electric.csv"
        ):
            raw_reading = to_raw_reading(row, "forward", 4)
            raw_readings[raw_reading.date].append(raw_reading)

        expected = {
            14: 2071.68,  # 3/14
            15: 2390.40,  # 3/15
            16: 2460.80,  # 3/17
        }
        for day in range(14, 17):
            dt = date(2021, 3, day)
            self.assertAlmostEqual(
                expected[day], sum([r.value for r in raw_readings[dt]]), 1
            )

    def test_parse_15_min_electric_dst(self):
        timeline = Timeline(date(2021, 3, 12), date(2021, 3, 16), interval=15)
        for row in extract_csv_rows(
            "datafeeds/scrapers/tests/fixtures/sdge_15_min_electric_dst.csv"
        ):
            raw_reading = to_raw_reading(row, "forward", 4)
            dt = datetime.combine(raw_reading.date, raw_reading.time)
            val = timeline.lookup(dt)
            if val:
                timeline.insert(dt, (raw_reading.value + val) / 2)
            else:
                timeline.insert(dt, raw_reading.value)

        # DST in this fixture is 2021-03-14
        data = timeline.serialize()
        dst_day = data["2021-03-14"]
        self.assertEqual(
            [
                4.86,
                4.9,
                4.42,
                2.14,
                3.84,
                4.9,
                5.68,
                5.08,
                None,
                None,
                None,
                None,
                2.8,
                2.22,
                5.86,
            ],
            dst_day[:15],
        )

    def test_parse_15_min_xlsx(self):
        timeline = Timeline(date(2021, 3, 31), date(2021, 3, 31), interval=15)
        parse_xlsx(timeline, "datafeeds/scrapers/tests/fixtures/sdge_15_min.xlsx", 4)
        data = timeline.serialize()
        self.assertEqual({"2021-03-31"}, set(data.keys()))
        day = data["2021-03-31"]
        self.assertEqual(96, len(day))
        self.assertAlmostEqual(7.36 * 4, day[0], 2)
        self.assertAlmostEqual(7.04 * 4, day[1], 2)
        self.assertAlmostEqual(7.04 * 4, day[2], 2)

    def test_parse_daily_xlsx(self):
        timeline = Timeline(date(2021, 3, 30), date(2021, 4, 5), interval=1440)
        parse_xlsx(timeline, "datafeeds/scrapers/tests/fixtures/sdge_daily_gas.xlsx", 1)
        data = timeline.serialize()
        self.assertEqual(
            set(
                [
                    "2021-03-30",
                    "2021-03-31",
                    "2021-04-01",
                    "2021-04-02",
                    "2021-04-03",
                    "2021-04-04",
                    "2021-04-05",
                ]
            ),
            set(data.keys()),
        )
        for key in data:
            self.assertEqual(1, len(data[key]))
        self.assertAlmostEqual(45.937, data["2021-03-30"][0], 2)
        self.assertAlmostEqual(42.168, data["2021-03-31"][0], 2)
        self.assertAlmostEqual(54.411, data["2021-04-01"][0], 2)
