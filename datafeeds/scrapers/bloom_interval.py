import logging
import xlrd

from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from datafeeds.common.base import BaseWebScraper, CSSSelectorBasePageObject

from datafeeds.common.exceptions import ApiError, LoginError
from datafeeds.common.support import Configuration, DateRange, Results
from datafeeds.common.timeline import Timeline
from datafeeds.common.util.selenium import clear_downloads

log = logging.getLogger(__name__)
DATE_FORMAT = "%m-%d-%Y"
MAX_DOWNLOAD_DAYS = 178  # Max days set by Bloom minus one to get full previous day
INTERVAL = 15


class BloomGridConfiguration(Configuration):
    def __init__(self, site_name: str):
        super().__init__(scrape_readings=True)
        self.site_name = site_name


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
    ReportTabSelector = ".sidenav ul li:nth-child(3) a.nav-dropdown-toggle"
    DataTabSelector = ".sidenav li.open ul.nav-dropdown-items li:nth-child(2)"

    def go_to_data_extract(self):
        self.find_element(self.ReportTabSelector).click()
        self._driver.wait().until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, self.DataTabSelector))
        )
        self.find_element(self.DataTabSelector).click()


class DataExtractPage(CSSSelectorBasePageObject):
    SiteSelect = 'site-fleet-select[name="locations"] .c-btn'
    SiteList = 'site-fleet-select[name="locations"] .c-list'
    MetricSelect = 'angular2-multiselect[name="metrics"] .c-btn'
    MetricList = 'angular2-multiselect[name="metrics"] .c-list'
    IntervalRadio = 'label[for="timeInterval-15min"]'
    CustomRadio = 'label[for="timescale-custom"]'
    FromDate = 'input[name="fromDate"]'
    ToDate = 'input[name="toDate"]'
    SubmitButton = "app-loading-button .btn-success"

    def find_text_for_checkbox(self, text: str):
        label = self._driver.find("//*[contains(text(), '{}')]".format(text), True)
        if label:
            label.click()
        else:
            raise ApiError("Label containing text '{}' not found".format(text))

    def get_earliest_year(self, page):
        parent = self.find_element(self.FromDate).find_element_by_xpath("..")
        parent.find_element_by_css_selector(".input-group-append").click()
        page.wait_until_ready("select")
        from_year = parent.find_element_by_xpath('//select[@title="Select year"]')

        return int(from_year.find_elements_by_tag_name("option")[0].text)

    def handle_multiselect(self, select: str, select_list: str, text: str):
        self.find_element(select).click()
        self.find_text_for_checkbox(text)
        # Hide popover
        self.find_element(select_list).click()

    def handle_radio_buttons(self, radio_button: str):
        self.find_element(radio_button).click()

    def fill_in_dates(self, start_date: str, end_date: str):
        self._driver.clear(self.FromDate)
        self._driver.fill(self.FromDate, start_date)
        self._driver.clear(self.ToDate)
        self._driver.fill(self.ToDate, end_date)
        self.find_element(self.SubmitButton).click()


class ExcelParser:
    def __init__(self, file_path: str, sheet_index: int = 0):
        self.file_path = file_path
        self.xl_workbook = xlrd.open_workbook(self.file_path)
        self.xl_sheet = self.xl_workbook.sheet_by_index(sheet_index)
        self.date_col = 1
        self.value_col = 2

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

    def parse_kwh(self, row_number: int):
        return self._convert_kwh_to_kw(
            self.xl_sheet.cell(row_number, self.value_col).value
        )

    @staticmethod
    def _convert_kwh_to_kw(kwh: float):
        # 15 minute intervals so multiply by 60 minutes and divide by interval
        return (kwh * 60) / INTERVAL


class BloomScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Bloom"
        self.site_url = "https://portal.bloomenergy.com/login"
        self.timeline = None

    @property
    def site_name(self):
        return self._configuration.site_name

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

    def _process_excel_file(self, file_path: str, start_date: date):
        parser = ExcelParser(file_path)
        start_row = parser.find_starting_date()

        if start_row:
            # Create timeline when we find the first date
            if not self.timeline:
                new_date = parser.parse_date(start_row).date()
                if new_date < start_date:
                    new_date = start_date
                self.timeline = Timeline(new_date, self.end_date)

            for row_number in range(start_row, parser.xl_sheet.nrows):
                date_value = parser.parse_date(row_number)

                raw_kwh = parser.parse_kwh(row_number)
                self.timeline.insert(date_value, raw_kwh)

    def _execute(self):
        self._driver.get(self.site_url)

        login_page = LoginPage(self._driver)
        landing_page = LandingPage(self._driver)
        extract_page = DataExtractPage(self._driver)

        login_page.wait_until_ready(login_page.SigninButtonSelector)
        self.screenshot("before login")
        login_page.login(self.username, self.password)

        self.screenshot("after login")
        landing_page.wait_until_ready(
            landing_page.ReportTabSelector,
            error_selector=login_page.ErrorMessage,
            error_cls=LoginError,
            error_msg="User ID and/or password not found.",
        )
        landing_page.go_to_data_extract()

        log.info("Filling out data extract form")

        self.screenshot("data extract page")
        # Verify dates and break into date ranges
        start_year = extract_page.get_earliest_year(extract_page)
        self.adjust_start_and_end_dates(start_year)
        date_range = DateRange(self.start_date, self.end_date)
        interval_size = relativedelta(days=MAX_DOWNLOAD_DAYS)

        extract_page.wait_until_ready(extract_page.SiteSelect)
        extract_page.handle_multiselect(
            extract_page.SiteSelect, extract_page.SiteList, self.site_name
        )
        # Metric multi-select
        extract_page.handle_multiselect(
            extract_page.MetricSelect,
            extract_page.MetricList,
            "Fuel Cell Energy Generation",
        )
        extract_page.handle_radio_buttons(extract_page.IntervalRadio)
        extract_page.handle_radio_buttons(extract_page.CustomRadio)

        for sub_range in date_range.split_iter(delta=interval_size):
            extract_page.wait_until_ready(extract_page.FromDate)
            # Minus one day because it will be missing midnight value for that day
            extract_page.fill_in_dates(
                self._format_date(sub_range.start_date - timedelta(days=1)),
                self._format_date(sub_range.end_date),
            )
            excel_filename = self.download_file("xlsx")

            self._process_excel_file(excel_filename, sub_range.start_date)

            log.info("Cleaning up download.")
            clear_downloads(self._driver.download_dir)

        readings = []
        if self.timeline:
            readings = self.timeline.serialize()
        return Results(readings=readings)
