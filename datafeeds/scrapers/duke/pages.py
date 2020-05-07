""" Duke Page module """
import time
import logging
import re
from datetime import datetime
from typing import List

from dateutil.parser import parse as parse_date

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from datafeeds import config
from datafeeds.common.util.selenium import window_count_equals
from datafeeds.common.util.selenium import WindowSwitch
from datafeeds.common.typing import BillingDatum
from datafeeds.common.util.selenium import ec_and
from datafeeds.common.util.pagestate.pagestate import PageState
from datafeeds.scrapers.duke import errors
from datafeeds.common.util.selenium import whole_page_screenshot
from datafeeds.common.upload import hash_bill_datum, upload_bill_to_s3


logger = None
log = logging.getLogger(__name__)


class DukeLoginPage(PageState):
    UsernameInputLocator = (By.CSS_SELECTOR, "input#username")
    PasswordInputLocator = (By.CSS_SELECTOR, "input#password")
    SubmitButtonLocator = (By.CSS_SELECTOR, "#mobile-login > div.text-center > button")

    def __init__(self, driver):
        super().__init__(driver)

    def get_ready_condition(self):
        return ec_and(
            EC.element_to_be_clickable(self.UsernameInputLocator),
            EC.element_to_be_clickable(self.PasswordInputLocator),
            EC.presence_of_element_located(self.SubmitButtonLocator),
        )

    def login(self, username: str, password: str):
        """Log into the Duke account """
        time.sleep(15)
        username_field = self.driver.find_element(*self.UsernameInputLocator)
        username_field.send_keys(username)

        password_field = self.driver.find_element(*self.PasswordInputLocator)
        password_field.send_keys(password)

        submit_button = self.driver.find_element(*self.SubmitButtonLocator)
        submit_button.click()


class DukeLoginFailedPage(PageState):
    """Page object representing a failed Duke login

    A failed login should produce an error message.
    This class facilitates detecting and retrieving that error message.
    """

    LoginErrorLocator = (By.XPATH, "//p[contains(text(), 'match our records')]")

    def __init__(self, driver):
        super().__init__(driver)

    def get_ready_condition(self):
        return ec_and(
            EC.title_contains("Sign In to Your Account"),
            EC.presence_of_element_located(self.LoginErrorLocator),
        )

    def raise_on_error(self):
        """Raise an exception describing the login error."""
        error = self.driver.find_element(*self.LoginErrorLocator)
        message = "Login failed. The website error is: '{}'".format(error.text)
        raise errors.BillingScraperLoginException(message)


class DukeLandingPage(PageState):
    """Page object for the Duke Energy landing page """

    def __init__(self, driver):
        super().__init__(driver)
        self.link_to_accs_locator = None

    def get_ready_condition(self):
        self.link_to_accs_locator = (By.CSS_SELECTOR, "button#btnBillView")
        return EC.element_to_be_clickable(self.link_to_accs_locator)

    def open_accounts_page(self):
        """Opens page with all the accounts """
        log.info("In landing page")
        bills_page_for_meters_link = self.driver.find_element(
            *self.link_to_accs_locator
        )
        bills_page_for_meters_link.click()

    def open_profiler_page(self):
        log.info("opening profilers page")
        profiler_locator = (By.CSS_SELECTOR, "a#lnkInternalMeter")
        profiler_page_link = self.driver.find_element(*profiler_locator)
        profiler_page_link.click()


class DukeAccountsPage(PageState):
    """Page object for the Duke Energy accounts page
        This page contains a list of all the accounts
    """

    def __init__(self, driver, utility: str, account_id: str):
        super().__init__(driver)
        self.accounts_window = None
        self.bill_info_list: List[BillingDatum] = []
        self.utility = utility
        self.account_id = account_id

    @staticmethod
    def isfloat(value):
        try:
            float(value)
            return True
        except ValueError:
            return False

    def get_ready_condition(self):
        if window_count_equals(1):
            return False
        accounts_header_locator = (By.XPATH, "//*[@id='subAccountListHeader']")
        with WindowSwitch(self.driver, self.accounts_window):
            return EC.presence_of_element_located(accounts_header_locator)

    def _get_text(self, locator_str, desc):
        elem_locator = (By.XPATH, locator_str)
        try:
            element = self.driver.find_element(*elem_locator)
        except:  # noqa E722
            return None
        return element.text.strip()

    def _get_number_of_entries(self):
        acct_info_locator = "//*[@id='billViewAccounts_info']"
        try:
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.presence_of_element_located((By.XPATH, acct_info_locator)))
        except Exception as e:
            raise errors.BillingScraperException(
                "Could not get Number of Accounts"
            ) from e
        accounts_info_locator = (By.XPATH, acct_info_locator)
        accounts_info_element = self.driver.find_element(*accounts_info_locator)
        accounts_info = accounts_info_element.text.strip()
        entries = re.findall(r"\d*\sentries$", accounts_info)
        if not entries:
            log.info("The scraper did not find any more accounts to process")
            return 0

        num_of_entries = entries[0].split()[0]  # ex: 10 entries
        return int(num_of_entries)

    @staticmethod
    def _get_dates(bill_date, date_line):
        """Usage dates do not include they year, so we have to deduce from bill date.
            example:
            If the bill date is Jan 01 2019, but usage starts in DEC,
            usage start date is DEC 2018
        """
        bill_date_split = bill_date.split(sep="/")  # mm/dd/YYYY
        if len(bill_date_split) < 3:
            raise errors.BillingScraperFormatException(
                "Bill date with unexpected format: %s " % bill_date
            )

        bill_date_month = bill_date_split[0]
        bill_date_year = bill_date_split[2]

        date_line_split = date_line.split()
        if len(date_line_split) < 9:
            raise errors.BillingScraperFormatException(
                "Date line with unexpected format: %s " % date_line
            )

        start_date_day = date_line_split[3]
        start_date_month = date_line_split[2]
        start_date_year = bill_date_year

        end_date_day = date_line_split[6]
        end_date_month = date_line_split[5]
        end_date_year = bill_date_year

        if (
            start_date_month == "DEC" or start_date_month == "NOV"
        ) and bill_date_month == "01":
            year = int(bill_date_year) - 1
            start_date_year = str(year)

        if end_date_month == "DEC" and bill_date_month == "01":
            year = int(bill_date_year) - 1
            end_date_year = str(year)

        start_date = parse_date(
            "%s/%s/%s" % (start_date_day, start_date_month, start_date_year)
        )
        end_date = parse_date(
            "%s/%s/%s" % (end_date_day, end_date_month, end_date_year)
        )
        return start_date.date(), end_date.date()

    def _is_short_bill(self):
        # Long bill formats contain a table with a more complex bill
        # That includes peak information
        long_bill_table_locator = "//*[@id='simple']//table[11]"
        try:
            self.driver.find_element_by_xpath(long_bill_table_locator)
        except:  # noqa E722
            return True

        return False

    def _scrape_bill(self, service_id, bill_link_text):
        bill_date = bill_link_text
        self.driver.implicitly_wait(3)
        link = self.driver.find_element_by_partial_link_text(bill_date)
        link.click()
        time.sleep(3)
        try:
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.presence_of_element_located((By.ID, "billImageToPrint")))
        except Exception as e:
            raise errors.BillingScraperPageNotFoundException(
                "Didn't find bill page with link %s" % bill_link_text
            ) from e

        # There are two bill formats ad Duke Energy
        short_bill = self._is_short_bill()
        if short_bill:
            return self._scrape_short_bill(service_id, bill_date)

        return self._scrape_long_bill(service_id, bill_date)

    def _scrape_short_bill(self, service_id, bill_date):
        """Scrape a bill and return an instance of BillingDatum"""
        table_locator = "//*[@id='simple']//table[4]//tr[%s]"
        peak_demand = self._get_peak_demand_in_bill(
            table_locator, service_id, bill_date
        )
        date_locator = "//*[@id='simple']/div[2]/div[4]/table[2]/tbody/tr/td[1]"
        charges_locator = "//*[@id='simple']//table[3]/tbody/tr[2]/td[3]/em"
        usage_locator = "//*[@id='simple']//table[5]/tbody/tr/td[1]/table//tr[2]"
        date_line = self._get_text(date_locator, "date")
        charges = self._get_text(charges_locator, "charges")
        # Remove the $ sign
        charges = charges.replace("$", "").replace(",", "")
        usage = self._get_text(usage_locator, "usage")
        # Remove the unit type (ie: KWH)
        if not usage.split():
            usage = 0
        else:
            usage = usage.split()[2].replace(",", "")
        return self._build_bill_datum(
            bill_date, charges, date_line, service_id, usage, peak_demand
        )

    def _get_number_of_rows_in_table(self, table_locator):
        rows_num = 0
        found_row = True
        while found_row:
            rows_num = rows_num + 1
            elem_locator = (By.XPATH, table_locator % str(rows_num))
            try:
                self.driver.find_element(*elem_locator)
            except:  # noqa E722
                found_row = False
        return rows_num

    def _get_peak_demand_in_bill(self, bill_table_row_locator, service_id, bill_date):
        KW = "kw"
        demand_values = []
        rows_num = self._get_number_of_rows_in_table(bill_table_row_locator)
        for row_num in range(1, rows_num):
            try:
                row_txt = self._get_text(bill_table_row_locator % str(row_num), " kw ")
            except Exception:
                log.warning(
                    "Unexpected error extracting text for account %s-%s"
                    % (service_id, bill_date)
                )
            row_txt = row_txt.lower()
            row_items = row_txt.split()
            # Check if found a row with a demand value (has a kw)
            if KW in row_txt and KW in row_items:  # We expect: 203,000 KW On-Peak ....
                kw_index = row_items.index(KW)
                if kw_index >= 1:
                    peak_demand = row_items[kw_index - 1]
                    peak_demand = peak_demand.replace(",", "")
                    # Append to the list of demand values
                    try:
                        demand_values.append(float(peak_demand))
                    except Exception:
                        # Found an invalid peak - pass
                        pass

        if not demand_values:
            log.warning(
                "Didn't find peak demand in bill for account %s-%s"
                % (service_id, bill_date)
            )
            return None

        # We are looking for the peak demand value
        return max(demand_values)

    def _scrape_long_bill(self, service_id, bill_date):
        """Scrape a bill and return an instance of BillingDatum"""
        # This bill format has a second table that we need to scrape
        # to find peak usage
        second_table_locator = "//*[@id='simple']//table[11]//tr[%s]"
        peak_demand = self._get_peak_demand_in_bill(
            second_table_locator, service_id, bill_date
        )
        date_locator = "//*[@id='simple']/div[2]/div[4]/table[2]/tbody/tr/td[1]"
        charges_locator = "//*[@id='simple']//table[3]/tbody/tr[2]/td[3]/em"
        usage_locator = "//*[@id='simple']//table[5]/tbody/tr/td[1]/table//tr[2]"
        date_line = self._get_text(date_locator, "date")
        charges = self._get_text(charges_locator, "charges")
        # Remove the $ sign
        charges = charges.replace("$", "").replace(",", "")
        usage = self._get_text(usage_locator, "usage")
        # Remove the unit type (ie: KWH)
        if not usage.split():
            usage = 0
        else:
            usage = usage.split()[2].replace(",", "")
        return self._build_bill_datum(
            bill_date, charges, date_line, service_id, usage, peak_demand
        )

    def _build_bill_datum(
        self, bill_date, charges, date_line, service_id, usage, peak=None
    ):
        start_date, end_date = self._get_dates(bill_date, date_line)

        try:
            usage_f = float(usage)
        except ValueError:
            usage_f = None

        bill_data = BillingDatum(
            start=start_date,
            end=end_date,
            cost=float(charges),
            used=usage_f,
            peak=peak,
            items=None,
            attachments=None,
        )
        # Add attachment
        bill_path = self.create_pdf_attachment(bill_date)
        with open(bill_path, "rb") as bill_file:
            key = hash_bill_datum(service_id, bill_data) + ".pdf"
            return bill_data._replace(
                attachments=[
                    upload_bill_to_s3(
                        bill_file,
                        key,
                        source="duke-energy.com",
                        statement=parse_date(bill_date).date(),
                        utility=self.utility,
                        utility_account_id=self.account_id,
                    )
                ]
            )

    def _go_to_account_bills_page(self):
        account_page_link = self.driver.find_element_by_id("billInformation")
        account_page_link.click()
        try:
            wait = WebDriverWait(self.driver, 10)
            wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "bill-view-link"))
            )
        except Exception as e:
            raise errors.BillingScraperPageNotFoundException(
                "Didn't find Account bills page"
            ) from e

    def _bill_in_date_range(self, billing_start, billing_end, date_str):
        """Check if the bill date is in the range of start date and end date"""
        log.info("Checking if date in range %s" % date_str)
        try:
            bill_date = datetime.strptime(date_str, "%m/%d/%Y").date()
            # billing start could be None
            if billing_start and bill_date < billing_start:
                return False
            # billing_end should not be None
            if bill_date <= billing_end:
                return True

            return False
        except ValueError:
            return False  # Found an element that is not a date

    def _scrape_account(self, service_id, link, billing_start, billing_end):
        """Scrape bills in one account"""
        log.info("About to scrape account")
        link.click()
        time.sleep(3)
        try:
            wait = WebDriverWait(self.driver, 10)
            wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "bill-view-link"))
            )
        except Exception as e:
            raise errors.BillingScraperPageNotFoundException(
                "Didn't find Account bill page"
            ) from e

        # Loop through all the bills:  bill-view-link
        bills_locator = (By.CLASS_NAME, "bill-view-link")
        bills_link_list = self.driver.find_elements(*bills_locator)
        bill_link_text_list = [link.text.strip() for link in bills_link_list]
        # Loop through all the bills:
        for bill_link_text in bill_link_text_list:
            if self._bill_in_date_range(billing_start, billing_end, bill_link_text):
                # Append the scraped bill information to bill_info_list
                bill_info = self._scrape_bill(service_id, bill_link_text)
                self.bill_info_list.append(bill_info)
                self._go_to_account_bills_page()

    def _go_back_to_accounts_page(self):
        """Once the bills in an account are processed,
            go back to the accounts page
        """
        log.info("Going back to the accounts page")
        with WindowSwitch(self.driver, self.accounts_window):
            account_page_link = self.driver.find_element_by_id("accounts")
            account_page_link.click()
            time.sleep(3)
            try:
                wait = WebDriverWait(self.driver, 10)
                wait.until(EC.presence_of_element_located((By.ID, "accountListHeader")))
            except Exception as e:
                raise errors.BillingScraperPageNotFoundException(
                    "Didn't find Account page"
                ) from e

    def process_account(self, service_id, billing_start, billing_end):
        """Scrapes bills for one account in the accounts table"""
        service_id = str(service_id)
        log.info("About to scrape account %s" % service_id)
        self.accounts_window = self.driver.window_handles[1]
        with WindowSwitch(self.driver, self.accounts_window):
            try:
                wait = WebDriverWait(self.driver, 10)
                wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, "//*[@id='billViewAccounts_filter']/label/input")
                    )
                )
            except Exception as e:
                raise errors.BillingScraperPageNotFoundException(
                    "Didn't find Account page"
                ) from e
            account_search_locator = (
                By.XPATH,
                "//*[@id='billViewAccounts_filter']/label/input",
            )
            account_search_box = self.driver.find_element(*account_search_locator)
            account_search_box.send_keys(service_id)
            time.sleep(1)
            acc_link_locator = "//*[@id='billViewAccounts']/tbody/tr[1]/td[5]/a"
            try:
                account_link = self.driver.find_element_by_xpath(acc_link_locator)
                self._scrape_account(
                    service_id, account_link, billing_start, billing_end
                )
                self._go_back_to_accounts_page()
            except Exception as e:
                raise errors.BillingScraperPageNotFoundException(
                    "Could not find account with service ID %s" % service_id
                ) from e
        return self.bill_info_list

    def process_all_accounts(self, billing_start, billing_end):
        """Scrapes bills for all the accounts in the accounts table
            between billing_start and billing_end
        """
        log.info("About to scrape all accounts")

        accts_name_locator_str = "//*[@id='billViewAccounts']/tbody/tr[%s]/td[4]"
        accts_locator_str = "//*[@id='billViewAccounts']/tbody//td[5]/a"
        accs_link_locator_str = "//*[@id='billViewAccounts']/tbody/tr[%s]/td[5]/a"
        self.accounts_window = self.driver.window_handles[1]
        with WindowSwitch(self.driver, self.accounts_window):
            number_of_entries = self._get_number_of_entries()
            curr_entry = 0
            while curr_entry < number_of_entries:
                # Get list of entries
                accounts_entries_locator = (By.XPATH, accts_locator_str)
                account_link_list = self.driver.find_elements(*accounts_entries_locator)
                num_of_visible_accounts = len(account_link_list)
                for acc_row_num in range(1, num_of_visible_accounts + 1):
                    # Get the service ID (or account number)
                    accts_name_locator = accts_name_locator_str % str(acc_row_num)
                    account_name = self.driver.find_element_by_xpath(accts_name_locator)
                    service_id = account_name.text.strip()
                    # Get the link to the account
                    acc_link_locator = accs_link_locator_str % str(acc_row_num)
                    account_link = self.driver.find_element_by_xpath(acc_link_locator)
                    # Scrape bills in this account
                    self._scrape_account(
                        service_id, account_link, billing_start, billing_end
                    )
                    self._go_back_to_accounts_page()
                    curr_entry = curr_entry + 1
                if curr_entry < number_of_entries:
                    next_btn_locator = "//*[@id='billViewAccounts_next']"
                    next_btn = self.driver.find_element_by_xpath(next_btn_locator)
                    next_btn.click()
                    time.sleep(2)
        return self.bill_info_list

    def _with_path(self, filename):
        return "{}/{}".format(config.WORKING_DIRECTORY, filename)

    def create_pdf_attachment(self, bill_date_str):
        bill_date_str = bill_date_str.replace("/", "_")
        outpath = self._with_path("bill_{}.pdf".format(bill_date_str))
        whole_page_screenshot(self.driver, outpath)
        return outpath
