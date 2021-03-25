import logging
import time
from typing import Optional

import xlrd

from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from datafeeds.common.alert import post_slack_message
from datafeeds.common.base import BaseWebScraper, CSSSelectorBasePageObject
from datafeeds.common.batch import run_datafeed

from datafeeds import db
from datafeeds.common.exceptions import ApiError
from datafeeds.common.support import Configuration, DateRange, Results
from datafeeds.common.timeline import Timeline
from datafeeds.common.typing import Status
from datafeeds.common.util.selenium import clear_downloads
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)
DATE_FORMAT = "%m-%d-%Y"
MAX_DOWNLOAD_DAYS = 178  # Max days set by Bloom minus one to get full previous day


class NoIntervalDataException(Exception):
    pass


class BloomGridConfiguration(Configuration):
    def __init__(self, site_name: str, meter_oid: int):
        super().__init__(scrape_readings=True)
        self.site_name = site_name
        self.meter_oid = meter_oid


class LoginPage(CSSSelectorBasePageObject):
    UsernameFieldSelector = 'input.form-control[name="username"]'
    PasswordFieldSelector = 'input.form-control[type="password"]'
    SigninButtonSelector = 'button[type="submit"]'
    ErrorMessage = "div.error-message"

    def login(self, username: str, password: str):
        """Authenticate with the web page.

        Fill in the username, password, then click "Login"
        """
        log.info("Inserting credentials on login page.")
        self._driver.fill(self.UsernameFieldSelector, username)
        self._driver.fill(self.PasswordFieldSelector, password)
        self.find_element(self.SigninButtonSelector).click()


class LandingPage(CSSSelectorBasePageObject):
    ReportTabSelectorXpath = "//span[text()='Reports']"
    DataTabSelectorXpath = "//span[text()='Data Extract']"

    def go_to_data_extract(self):
        self._driver.wait().until(
            EC.element_to_be_clickable((By.XPATH, self.ReportTabSelectorXpath))
        )
        self._driver.find_element_by_xpath(self.ReportTabSelectorXpath).click()
        self._driver.wait().until(
            EC.element_to_be_clickable((By.XPATH, self.DataTabSelectorXpath))
        )
        self._driver.find_element_by_xpath(self.DataTabSelectorXpath).click()


class DataExtractPage(CSSSelectorBasePageObject):
    SiteSelect = 'site-fleet-select[name="locations"] .c-btn'
    SiteList = 'site-fleet-select[name="locations"] .c-list'
    MetricSelect = 'angular2-multiselect[name="metrics"] .c-btn'
    MetricList = 'angular2-multiselect[name="metrics"] .c-btn'
    IntervalRadio = 'label[for="timeInterval-15min"]'
    CustomRadio = 'label[for="timescale-custom"]'
    FromDate = 'input[name="dpFromDate"]'
    ToDate = 'input[name="dpToDate"]'
    SubmitButton = 'app-loading-button button[type="submit"]'
    CardHeader = ".card-header"

    def find_text_for_checkbox(self, text: str):
        label = self._driver.find("//*[contains(text(), '{}')]".format(text), True)
        if label:
            time.sleep(3)
            log.info("Clicking %s", text)
            label.click()
        else:
            raise ApiError("Label containing text '{}' not found".format(text))

    def get_earliest_year(self, page):
        parent = self.find_element(self.FromDate).find_element_by_xpath("..")
        parent.find_element_by_css_selector(".input-group-append").click()
        page.wait_until_ready("select")
        from_year = parent.find_element_by_xpath('//select[@title="Select year"]')

        return int(from_year.find_elements_by_tag_name("option")[0].text)

    def handle_multiselect(self, select: str, text: str):
        self.find_element(self.CardHeader).click()
        self.find_element(select).click()
        log.info("Select text %s", text)
        self.find_text_for_checkbox(text)

    def handle_radio_buttons(self, radio_button: str):
        self.find_element(radio_button).click()

    def fill_in_dates(self, start_date: str, end_date: str):
        self._driver.clear(self.FromDate)
        self._driver.fill(self.FromDate, start_date)
        self._driver.clear(self.ToDate)
        self._driver.fill(self.ToDate, end_date)

        self.find_element(self.CardHeader).click()
        self.find_element(self.SubmitButton).click()


class ExcelParser:
    def __init__(self, file_path: str, sheet_index: int = 0):
        self.file_path = file_path
        self.xl_workbook = xlrd.open_workbook(self.file_path)
        self.xl_sheet = self.xl_workbook.sheet_by_index(sheet_index)
        self.date_col = 1
        self.value_col = 5
        self.check_energy_column()

    def check_energy_column(self):
        log.info("Confirming energy output is column %s", self.value_col)
        output_string = "text:'Electricity Out'"

        for cell in range(len(self.xl_sheet.col(self.value_col))):
            if str(self.xl_sheet.col(self.value_col)[cell]) == output_string:
                log.info("Electricity output is column %s", self.value_col)
                return
        log.error("Did not find electricity output in column %s", self.value_col)
        raise Exception

    def find_starting_date(self):
        # Find the first date row where interval dates start.
        # Starts in second column since first is just the logo.
        for row_number in range(0, self.xl_sheet.nrows):
            if self.xl_sheet.cell(row_number, 1).ctype == xlrd.XL_CELL_DATE:
                return row_number

        return None

    def parse_date(self, row_number: int):
        return datetime(
            *xlrd.xldate_as_tuple(
                self.xl_sheet.cell(row_number, self.date_col).value,
                self.xl_workbook.datemode,
            )
        )

    def parse_kwh(self, row_number: int, interval: int):
        return self._convert_kwh_to_kw(
            self.xl_sheet.cell(row_number, self.value_col).value, interval
        )

    @staticmethod
    def _convert_kwh_to_kw(kwh: float, interval: int):
        # 15 minute intervals (or 1440 for 1st gen models) so multiply by 60 minutes and divide by interval
        return (kwh * 60) / interval


class BloomScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Bloom"
        self.site_url = "https://portal.bloomenergy.com/login"
        self.timeline = None

    @property
    def site_name(self):
        return self._configuration.site_name

    @property
    def meter_oid(self):
        return self._configuration.meter_oid

    def adjust_start_and_end_dates(self, start_year: int):
        """
        End date can be no later than today, and must be later
        than the start date.
        Dates cannot be more than 180 days apart.
        """
        today = datetime.today().date()
        earliest_date = date(int(start_year), 1, 1)
        if self.start_date < earliest_date:
            self.start_date = earliest_date
        if self.start_date > self.end_date:
            self.end_date = self.start_date + timedelta(days=1)
            log.info("Adjusting end date to {}".format(self.end_date))

        if self.end_date > today:
            self.end_date = today
            log.info("Adjusting end date to {}".format(self.end_date))

    @staticmethod
    def _format_date(date_to_format: datetime) -> str:
        return date_to_format.strftime(DATE_FORMAT)

    def _process_excel_file(self, file_path: str, start_date: date, interval) -> bool:
        parser = ExcelParser(file_path)
        start_row = parser.find_starting_date()
        if start_row:
            # Create timeline when we find the first date
            if not self.timeline:
                new_date = parser.parse_date(start_row).date()
                if new_date < start_date:
                    new_date = start_date
                self.timeline = Timeline(new_date, self.end_date, interval=interval)

            for row_number in range(start_row, parser.xl_sheet.nrows):
                date_value = parser.parse_date(row_number)

                raw_kwh = parser.parse_kwh(row_number, interval)
                self.timeline.insert(date_value, raw_kwh)
        return bool(start_row)

    def _get_meter_interval(self):
        meter = db.session.query(Meter).get(self.meter_oid)
        return meter.interval

    def _check_meter_interval(self, interval=1440):
        meter_interval = self._get_meter_interval()
        if meter_interval == interval:
            log.info("Meter interval is already %s", interval)
            return
        log.error("Interval for meter %s is %s", self.meter_oid, interval)
        post_slack_message(
            "Bloom meter %s returned empty interval data. It may only have daily values"
            % self.meter_oid,
            "#scrapers",
            ":exclamation:",
            username="Scraper monitor",
        )
        # An engineer will need to check the Bloom UI
        # and possibly change the meter interval in the database.
        raise NoIntervalDataException

    def _execute(self):
        self._driver.get(self.site_url)
        log.info(self._configuration.__dict__)
        log.info(self._configuration.meter_oid)
        interval = self._get_meter_interval()
        log.info("meter interval is %s", interval)

        login_page = LoginPage(self._driver)
        landing_page = LandingPage(self._driver)
        extract_page = DataExtractPage(self._driver)
        if interval == 1440:
            extract_page.IntervalRadio = 'label[for="timeInterval-daily"]'

        login_page.wait_until_ready(login_page.SigninButtonSelector)
        self.screenshot("before login")
        login_page.login(self.username, self.password)

        self.screenshot("after login")
        landing_page.go_to_data_extract()

        log.info("Filling out data extract form")

        self.screenshot("data extract page")
        # Verify dates and break into date ranges
        start_year = extract_page.get_earliest_year(extract_page)
        self.adjust_start_and_end_dates(start_year)
        date_range = DateRange(self.start_date, self.end_date)
        interval_size = relativedelta(days=MAX_DOWNLOAD_DAYS)

        readings = []

        self._export_data(extract_page, date_range, interval_size, interval=interval)

        if self.timeline:
            readings = self.timeline.serialize()

        return Results(readings=readings)

    def _export_data(self, extract_page, date_range, interval_size, interval):
        extract_page.wait_until_ready(extract_page.CustomRadio)
        extract_page.handle_multiselect(extract_page.SiteSelect, self.site_name)

        # Metric multi-select
        # The site seems to have changed at some point - all options are now enabled
        # However, the scraper fails without this step
        extract_page.handle_multiselect(
            extract_page.MetricSelect,
            "Fuel Cell Energy Generation",
        )

        extract_page.handle_radio_buttons(extract_page.IntervalRadio)
        extract_page.handle_radio_buttons(extract_page.CustomRadio)

        found_fifteen_minute_data = False
        for sub_range in date_range.split_iter(delta=interval_size):
            extract_page.wait_until_ready(extract_page.FromDate)
            # Minus one day because it will be missing midnight value for that day
            extract_page.fill_in_dates(
                self._format_date(sub_range.start_date - timedelta(days=1)),
                self._format_date(sub_range.end_date),
            )
            excel_filename = self.download_file("xlsx")

            status = self._process_excel_file(
                excel_filename, sub_range.start_date, interval
            )
            log.info("Cleaning up download.")
            clear_downloads(self._driver.download_dir)
            if status:
                found_fifteen_minute_data = True

            time.sleep(3)
        if not found_fifteen_minute_data:
            self._check_meter_interval(interval=1440)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    configuration = BloomGridConfiguration(
        site_name=datasource.meta.get("site_name"), meter_oid=meter.oid
    )

    return run_datafeed(
        BloomScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
