from datetime import date, datetime, timedelta
from time import sleep
import os
from typing import List, Optional, Tuple
import csv
import logging

from datafeeds import db
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.typing import Status
from datafeeds.common.timeline import Timeline
from datafeeds.common.support import Configuration, Results
from datafeeds.common.util.selenium import file_exists_in_dir, clear_downloads
from datafeeds.models import (
    SnapmeterAccount,
    SnapmeterAccountMeter,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)


logger = None
log = logging.getLogger(__name__)


class NautilusConfiguration(Configuration):
    def __init__(self, meter_id: str, account_id: str):
        super().__init__(scrape_readings=True)
        # meter id is long alphanumeric at end of site url + meter number (1,2,...)
        self.meter_id = meter_id
        # account id is subdomain at beginning of site url
        self.account_id = account_id


class NautilusException(Exception):
    pass


class CSVParser:
    def __init__(self, filepath: str, meter_number: str = "1"):
        # meter number is like 1,2...
        self.filepath = filepath
        self.meter_number = meter_number

    @staticmethod
    def csv_str_to_date(datestr: str) -> datetime:
        # eg, 2/4/2020 5:30 AM
        date_format = "%m/%d/%Y %I:%M %p"
        return datetime.strptime(datestr, date_format)

    def process_csv(self) -> List[Tuple[datetime, float]]:
        # 3 header rows:
        # 1st row is site name
        # second row is meter names - only one that matters here
        # third row is units - check that it's kW

        def check_units(header_row) -> bool:
            units_in_kw = False
            for i in header_row:
                if i.lower() == "kw":
                    units_in_kw = True
            if not units_in_kw:
                raise NautilusException("Error - Units not in kW")
            return units_in_kw

        def count_meters(header_row) -> int:
            # count row header for 'Meter' usage
            meters_detected = 0
            for i in header_row:
                if "Meter" in i:
                    meters_detected += 1
            if meters_detected == 0:
                raise NautilusException("Error - No meters detected")
            return meters_detected

        results = []
        msg = "Processing csv at %s" % self.filepath
        log.info(msg)

        with open(self.filepath, mode="r") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            header_1 = next(csv_reader)
            msg = "CSV header 1: %s" % header_1
            log.info(msg)
            header_2 = next(csv_reader)
            msg = "CSV header 2: %s" % header_2
            log.info(msg)
            header_3 = next(csv_reader)
            msg = "CSV header 3: %s" % header_3
            log.info(msg)

            number_of_meters = count_meters(header_2)
            msg = "Number of meters at site is %s" % number_of_meters
            log.info(msg)
            units_in_kw = check_units(header_3)
            if units_in_kw:
                msg = "Units are in %s" % units_in_kw
                log.info(msg)
            meter_number_int = int(self.meter_number)
            msg = "Meter number: %s" % meter_number_int
            # actually process the values
            for row in csv_reader:
                # columns are Date, Meter Readings, Inverter
                msg = "%s, %s" % (row[0], row[meter_number_int])
                log.debug(msg)
                # if CSV returns blank values for part of day, return
                if len(row[meter_number_int]) == 0:
                    break
                dt = self.csv_str_to_date(row[0])
                try:
                    kw = float(row[meter_number_int])
                # entire days can be missing. assume production is zero
                except ValueError:
                    kw = 0.0
                results.append((dt, kw))

        return results


class SitePage:
    def __init__(self, driver):
        self.driver = driver

    @staticmethod
    def string_to_date(date_string: str, date_format: str) -> date:
        return datetime.strptime(date_string, date_format).date()

    def five_days_select(self):
        five_days_xpath = '//*[@id="Chart_Query_Header"]/nav/ul/li[3]/a'
        five_days = self.driver.find_element_by_xpath(five_days_xpath)
        five_days.click()

    def month_select(self):
        month_xpath = '//*[@id="Chart_Query_Header"]/nav/ul/li[5]/a'
        month = self.driver.find_element_by_xpath(month_xpath)
        month.click()

    def double_back_arrow_select(self):
        double_arrow_xpath = '//*[@id="Chart_Query_Header"]/nav/ul/li[1]/div/a[1]/span'
        double_arrow = self.driver.find_element_by_xpath(double_arrow_xpath)
        double_arrow.click()

    def hamburger_select(self):
        # this is in an svg
        # https://payitforward14.blogspot.com/2015/06/how-to-write-xpath-for-svg-elements.html
        hamburger_xpath = "//*[name() = 'svg']/*[name() = 'g'][1]/*[name() = 'path']"
        hamburger = self.driver.find_element_by_xpath(hamburger_xpath)
        hamburger.click()

    def download_csv(self) -> str:
        # in an svg and the div id seems to change
        download_csv_xpath = (
            "//*[name() = 'div' and starts-with(@id, 'highcharts-')]"
            "/*[name() = 'div'][3]/*[name() = 'div']/*[name() = 'div'][2]"
        )
        download_csv = self.driver.find_element_by_xpath(download_csv_xpath)
        download_csv.click()
        download_dir = self.driver.download_dir
        filename = self.driver.wait(60).until(
            file_exists_in_dir(download_dir, r".*\.csv$")
        )
        file_path = os.path.join(download_dir, filename)
        return file_path

    def get_install_date(self) -> date:
        install_date_xpath = "//span[contains(text(), '/')]"
        install_date_str = self.driver.find_element_by_xpath(install_date_xpath).text
        msg = "Install date: %s" % install_date_str
        log.info(msg)
        date_format = "%m/%d/%Y"
        install_date = self.string_to_date(install_date_str, date_format)
        return install_date

    def get_earliest_shown(self) -> date:
        # also in svg namespace
        earliest_shown_xpath = "//*[name() = 'tspan']"
        # eg, "February 14 - 19, 2020"
        date_range_str = self.driver.find_element_by_xpath(earliest_shown_xpath).text

        # if cycling by months, date range string is like 'January, 2020
        date_format = "%B %d  %Y"
        if "–" not in date_range_str:
            earliest_shown_datestr = date_range_str.split(",")
            earliest_shown_datestr = (
                earliest_shown_datestr[0] + " 1 " + earliest_shown_datestr[1]
            )
            earliest_shown = self.string_to_date(earliest_shown_datestr, date_format)
            return earliest_shown

        earliest_shown_datestr = date_range_str.split("–")[
            0
        ]  # this is a dash, not a hyphen?

        # Catch a corner case eg, 'December 28, 2019 – January 1, 2020'
        if len(date_range_str.split(",")) == 2:
            year_str = date_range_str.split(",")[-1]
            earliest_shown_datestr = earliest_shown_datestr + year_str
        elif len(date_range_str.split(",")) == 3:
            log.info("Handling case where range is over 2 years")
            log.info(earliest_shown_datestr)
            earliest_shown_datestr = earliest_shown_datestr[:-1]
            log.info(earliest_shown_datestr)
            date_format = "%B %d, %Y"
        else:
            raise NautilusException("Problem parsing date string")

        earliest_shown = self.string_to_date(earliest_shown_datestr, date_format)
        return earliest_shown


class NautilusScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Nautilus Solar"
        self.browser_name = "Chrome"
        self.install_date = None
        self.readings = {}
        self.site_url = None

    def adjust_start_and_end_dates(self):
        if self.start_date < self.install_date:
            self.start_date = self.install_date
            log.info("Adjusting start date to {}.".format(self.start_date))

        today = datetime.today().date()
        if self.end_date > today:
            self.end_date = today
            log.info("Adjusting end date to {}".format(self.end_date))

        if self.start_date > self.end_date:
            self.end_date = self.start_date + timedelta(days=1)
            log.info("Adjusting end date to {}".format(self.end_date))

    def construct_site_url(self):
        meter_id = self._configuration.meter_id.split("-")[0]
        self.site_url = "http://{}.mini.alsoenergy.com/Dashboard/{}".format(
            self._configuration.account_id, meter_id
        )
        msg = "Site URL is: %s" % self.site_url
        log.info(msg)

    def _execute(self):
        self.construct_site_url()
        self._driver.get(self.site_url)
        sleep(5)
        if "Error?aspxerrorpath" in self._driver.current_url:
            raise NautilusException("Error - could not find site url")

        site_page = SitePage(self._driver)

        five_days = timedelta(days=5)
        self.install_date = site_page.get_install_date()
        msg = "Installation date is %s" % self.install_date
        self.adjust_start_and_end_dates()
        log.info(msg)

        site_page.month_select()
        sleep(5)

        earliest_shown = site_page.get_earliest_shown()
        # coarse-grained: go back by month
        while self.end_date < earliest_shown - timedelta(days=30):
            msg = "finding where to start. earliest_shown is %s" % earliest_shown
            log.info(msg)
            site_page.double_back_arrow_select()
            sleep(5)
            earliest_shown = site_page.get_earliest_shown()

        site_page.five_days_select()
        sleep(10)

        # fine-grained: go back by 5-day increments
        while self.end_date < (earliest_shown - five_days):
            msg = "finding where to start. earliest_shown is %s" % earliest_shown
            log.info(msg)
            site_page.double_back_arrow_select()
            sleep(5)
            earliest_shown = site_page.get_earliest_shown()

        timeline = Timeline(self.start_date, self.end_date)
        while (self.start_date - five_days) < earliest_shown:
            msg = "gathering data. start_date is %s" % self.start_date
            log.info(msg)
            msg = "gathering data. earliest_shown is %s" % earliest_shown
            log.info(msg)
            site_page.hamburger_select()
            sleep(2)
            file_path = site_page.download_csv()
            data = CSVParser(file_path).process_csv()
            for dt, use_kw in data:
                timeline.insert(dt, use_kw)
            log.info("\tRecorded %d intervals of data." % len(data))
            log.info("Cleaning up download.")
            clear_downloads(self._driver.download_dir)
            site_page.double_back_arrow_select()
            sleep(5)
            earliest_shown = site_page.get_earliest_shown()

        return Results(readings=timeline.serialize())


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    acct_meter = (
        db.session.query(SnapmeterAccountMeter)
        .filter_by(meter=meter.oid, account=account.oid)
        .first()
    )

    configuration = NautilusConfiguration(
        meter_id=meter.service_id, account_id=acct_meter.utility_account_id
    )

    return run_datafeed(
        NautilusScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
