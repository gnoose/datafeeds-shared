import os
import csv
import logging
from typing import List, Dict
from math import ceil

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import DateRange
from datafeeds.common.support import Results
from datafeeds.common.support import Configuration
from datafeeds.common.util.selenium import (
    IFrameSwitch,
    file_exists_in_dir,
    clear_downloads
)


log = logging.getLogger(__name__)
DATE_FORMAT = "%Y-%m-%d"
MAX_INTERVAL_LENGTH = 1


class SolrenGridConfiguration(Configuration):
    def __init__(self, inverter_id: str, site_id: str):
        super().__init__(scrape_readings=True)
        self.inverter_id = inverter_id
        # Used as a query param
        self.site_id = site_id


class OverviewPage:
    site_analytics_xpath = ".//a[contains(text(), 'Site Analytics')]"

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        with IFrameSwitch(self._driver, "childFrame"):
            log.info("Waiting to see the site analytics tab.")
            self._driver.wait().until(EC.presence_of_element_located(
                (By.XPATH, self.site_analytics_xpath))
            )

    def navigate_to_site_analytics(self):
        with IFrameSwitch(self._driver, "childFrame"):
            log.info("Clicking on the site analytics tab.")
            self._driver.click(self.site_analytics_xpath, xpath=True)


class SiteAnalyticsPage:
    dropdown_button_one_selector = "button.ui-multiselect:nth-of-type(1)"
    dropdown_button_two_selector = "button.ui-multiselect:nth-of-type(2)"
    block_ui_xpath = "//div[@class='blockUI blockOverlay']"
    lhs_li_xpath = "//input[contains(@name, 'multiselect_selectD1')][contains(@value, '{}')]/ancestor::li"
    rhs_li_xpath = "//input[contains(@name, 'multiselect_selectD2')][contains(@value, '{}')]/ancestor::li"

    ac_power_id = "btnACPowerId"
    install_date_selector = "input#installDate"

    def __init__(self, driver):
        self._driver = driver

    def wait_until_ready(self):
        with IFrameSwitch(self._driver, "childFrame"):
            # Page blocked by a loading indicator - wait for this to disappear
            self._driver.wait().until(EC.invisibility_of_element_located((By.XPATH, self.block_ui_xpath)))

    def _select_individual_inverter(self, dropdown_button: str,
                                    xpath: str, inverter_id: str):
        element = self._driver.wait().until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, dropdown_button))
        )
        element.click()
        try:
            self._driver.click(xpath.format(inverter_id), xpath=True)
        except NoSuchElementException:
            raise Exception("Inverter {} not found in dropdown".format(inverter_id))
        self._driver.sleep(1)

    def select_inverter_from_both_dropdowns(self, inverter_id: str):
        """
        SolrenView has two dropdowns to compare inverters. Since we're
        scraping one inverter at a time, we're selecting the same inverter
        from both dropdowns - this will also halve the amount of data we're
        requesting at a time.
        """
        with IFrameSwitch(self._driver, "childFrame"):
            with IFrameSwitch(self._driver, "frame3"):
                log.info("Selecting inverter from LHS dropdown.")
                self._select_individual_inverter(
                    self.dropdown_button_one_selector,
                    self.lhs_li_xpath,
                    inverter_id
                )

                log.info("Selecting same inverter from RHS dropdown.")
                self._select_individual_inverter(
                    self.dropdown_button_two_selector,
                    self.rhs_li_xpath,
                    inverter_id
                )

    def click_ac_power_button(self):
        """
        This switches data to kW format.
        """
        with IFrameSwitch(self._driver, "childFrame"):
            with IFrameSwitch(self._driver, "frame3"):
                log.info("Waiting to see AC Power Button.")
                self._driver.wait().until(EC.element_to_be_clickable(
                    (By.ID, self.ac_power_id))
                )
                log.info("Clicking AC Power button.")
                self._driver.find_element_by_id(self.ac_power_id).click()
                self._driver.sleep(1)

    def get_install_date(self):
        """
        Install date is the earliest date we can request -
        located in a hidden input element on the page.
        """
        with IFrameSwitch(self._driver, "childFrame"):
            with IFrameSwitch(self._driver, "frame3"):
                log.info("Finding inverter install date.")
                return self._driver.find_or_raise(
                    self.install_date_selector
                ).get_attribute("value")


class DatePickerSection:
    title_dialog_selector = "span#ui-dialog-title-dialog"
    range_button_selector = "button#Range"

    from_selector = "input#from.hasDatepicker"
    to_selector = "input#to.hasDatepicker"
    done_button_xpath = "//button[@type='button' and span='Done']"
    export_data_xpath = "//button[span='Export Data']"

    def __init__(self, driver):
        self._driver = driver

    @staticmethod
    def date_to_string(date_obj):
        return date_obj.strftime(DATE_FORMAT)

    def _click_range_button(self):
        """
        This opens a form with two datepickers.
        """
        with IFrameSwitch(self._driver, "childFrame"):
            with IFrameSwitch(self._driver, "frame3"):
                self._driver.wait().until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, self.range_button_selector))
                )
                log.info("Clicking on Range button.")
                self._driver.click(self.range_button_selector)
                self._driver.sleep(1)

    def _enter_date(self, date_obj: datetime, date_input_selector):
        with IFrameSwitch(self._driver, "childFrame"):
            with IFrameSwitch(self._driver, "frame3"):
                self._driver.wait().until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, date_input_selector))
                )
                self._driver.clear(date_input_selector)
                date_string = self.date_to_string(date_obj)
                log.info("Entering date {}".format(date_string))
                self._driver.fill(date_input_selector, date_string)

                # Click outside of the date picker, helps make "Done" button selectable
                self._driver.click(self.title_dialog_selector)
                self._driver.sleep(2)

    def _enter_start_date(self, date_obj: datetime):
        self._enter_date(date_obj, self.from_selector)

    def _enter_end_date(self, date_obj: datetime):
        self._enter_date(date_obj, self.to_selector)

    def _submit_form(self):
        log.info("Submitting dates.")
        with IFrameSwitch(self._driver, "childFrame"):
            with IFrameSwitch(self._driver, "frame3"):
                self._driver.wait().until(EC.element_to_be_clickable(
                    (By.XPATH, self.done_button_xpath))
                )
                self._driver.click(self.done_button_xpath, xpath=True)

    def _export_data(self) -> str:
        log.info("Exporting data.")
        with IFrameSwitch(self._driver, "childFrame"):
            with IFrameSwitch(self._driver, "frame3"):
                self._driver.wait().until(EC.element_to_be_clickable(
                    (By.XPATH, self.export_data_xpath))
                )
                self._driver.click(self.export_data_xpath, xpath=True)
                # Wait for csv to download
                download_dir = self._driver.download_dir
                filename = self._driver.wait(60).until(
                    file_exists_in_dir(download_dir, r".*\.csv$")
                )
                file_path = os.path.join(download_dir, filename)
                return file_path

    def complete_form_and_download(self, start: datetime, end: datetime) -> str:
        """
        Returns filepath for downloaded csv of AC power data
        with given start and end dates
        """
        log.info("---------------")
        self._click_range_button()
        self._enter_start_date(start)
        self._enter_end_date(end)
        self._submit_form()
        return self._export_data()


class CSVParser:
    def __init__(self, inverter_id: str, filepath: str):
        self.inverter_id = inverter_id
        self.filepath = filepath
        self.intermediate_readings = {}

    @staticmethod
    def _get_header_position(header_row: List[str], column_title: str) -> int:
        """
        Returns the desired column index, looking for any column header that
        *contains* the column title.

        Not assuming that csv headers, ordering will stay the same
        """
        for pos, column in enumerate(header_row):
            if column_title.lower() in column.lower():
                return pos

        raise Exception("Expected column header not found for {}".format(
            column_title)
        )

    @staticmethod
    def csv_str_to_date(datestr: str) -> datetime:
        """
        Converts the date from the csv into a datetime
        """
        return datetime.strptime(datestr, "%Y-%b-%d %I:%M %p")

    @staticmethod
    def date_to_final_str(date_obj: datetime) -> str:
        """
        Converts a datetime into the expected string format in the results
        """
        return date_obj.strftime("%Y-%m-%d")

    @staticmethod
    def date_to_intermediate_time_str(date_obj: datetime) -> str:
        return date_obj.strftime("%H:%M")

    def build_intermediate_dict(self) -> Dict[str, float]:
        """
        Returns a default starting dictionary with 96 times in fifteen minute
        intervals, starting at midnight, with power values of 0.0.

        Not all times are returned in SolrenView CSV's - they often start at
        4:30 AM and end at 10:30 PM. So we begin with a dictionary with all times
        populated.
        Ex: {
            "00:00": 0.0,
            "00:05": 0.0,
            ...
        }
        """
        intermediate = {}
        now = datetime.now()
        current_time = now.replace(hour=0, minute=0, second=0, microsecond=0)

        delta = timedelta(minutes=15)

        for i in range(0, 96):
            intermediate[self.date_to_intermediate_time_str(current_time)] = 0.0
            current_time = current_time + delta

        return intermediate

    def round_up_to_quarter_hour(self, dt: datetime) -> str:
        """
        Rounds the time up to the nearest fifteen minutes.
        Ex:
            7:05 -> 07:15
            7:20 -> 07:30
            7:50 -> 08:00

        For two-day intervals, SolrenView power data is returned in five-minute
        intervals. The five minute intervals need to be summed to fifteen minutes.
        :returns human-readable time in HH:MM
        """
        delta = timedelta(minutes=15)
        # Round time backwards to the hour
        rounded_hour = dt.replace(minute=0, second=0, microsecond=0)
        rounded_qtr_hour = rounded_hour + ceil((dt - rounded_hour) / delta) * delta
        return self.date_to_intermediate_time_str(rounded_qtr_hour)

    def finalize_readings(self):
        """
        Transforms the readings so dates are keyed to an array of power values.
        """
        finalized_readings = {}
        for reading in self.intermediate_readings:
            finalized_readings[reading] = list(
                self.intermediate_readings[reading].values()
            )
        return finalized_readings

    def process_csv(self):
        """
        For each day, groups 5-minute power readings into fifteen minute intervals.
        Returns a dictionary with dates keyed to arrays of 96 power values.
        ex.
        {
            "YYYY-MM-DD": ["100.31", "432.03", ...],
            "YYYY-MM-DD": ["104.33", "99.78", ...],
        }
        """
        with open(self.filepath, mode="r") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            header = next(csv_reader)

            date_idx = self._get_header_position(header, "Label")
            power_idx = self._get_header_position(header, "kW")

            if self.inverter_id not in header[power_idx]:
                raise Exception("Inverter data returned for the incorrect meter.")

            for row in csv_reader:
                date_obj = self.csv_str_to_date(row[date_idx])
                power = float(row[power_idx] or 0)

                current_date = self.date_to_final_str(date_obj)
                current_time = self.round_up_to_quarter_hour(date_obj)

                if current_date not in self.intermediate_readings:
                    self.intermediate_readings[current_date] = self.build_intermediate_dict()

                current_reading = self.intermediate_readings[current_date][current_time]
                # Here's where we sum power readings together - rounded to fifteen min intervals
                self.intermediate_readings[current_date][current_time] = round(
                    float(current_reading + power), 2
                )

        return self.finalize_readings()


class SolrenScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Solren"
        self.site_url = "https://www.solrenview.com/SolrenView/mainFr.php?siteId={}".format(
            self._configuration.site_id
        )
        self.install_date = None
        self.readings = {}

    @property
    def inverter_id(self):
        return self._configuration.inverter_id

    @staticmethod
    def string_to_date(date_str):
        return datetime.strptime(date_str, DATE_FORMAT).date()

    def adjust_start_and_end_dates(self):
        """
        Start date can be no earlier than the inverter install date.
        End date can be no later than today, and must be later
        than the start date.
        """
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

    def _execute(self):
        # Direct driver to site url -
        # Currently a public URL, no credentials needed. Will have to be
        # refactored in the future if we start scraping private sites.
        self._driver.get(self.site_url)

        # Create page helpers
        overview_page = OverviewPage(self._driver)
        site_analytics_page = SiteAnalyticsPage(self._driver)
        date_picker_component = DatePickerSection(self._driver)

        # Navigate to site analytics tab
        overview_page.wait_until_ready()
        self.screenshot("before clicking on site analytics tab")
        overview_page.navigate_to_site_analytics()

        # Select inverter from both dropdowns
        site_analytics_page.wait_until_ready()
        self.screenshot("before selecting inverters")
        site_analytics_page.select_inverter_from_both_dropdowns(self.inverter_id)
        # Click on AC Power button
        self.screenshot("before clicking on ac power button")
        site_analytics_page.click_ac_power_button()
        self.screenshot("after clicking on ac power button")
        self.install_date = self.string_to_date(
            site_analytics_page.get_install_date()
        )

        # Adjust start and end date, depending on inverter install date
        self.adjust_start_and_end_dates()

        date_range = DateRange(self.start_date, self.end_date)
        interval_size = relativedelta(days=MAX_INTERVAL_LENGTH)

        # Loop through desired interval in two day chunks to pull down
        # power generated
        for sub_range in date_range.split_iter(delta=interval_size):
            start = sub_range.start_date
            end = sub_range.end_date

            file_path = date_picker_component.complete_form_and_download(start, end)

            intermediate_readings = CSVParser(self.inverter_id, file_path).process_csv()
            self.readings.update(intermediate_readings)

            log.info("Cleaning up download.")
            clear_downloads(self._driver.download_dir)
            # Adding a large pause
            self._driver.sleep(5)

        return Results(readings=self.readings)
