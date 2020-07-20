import time
import collections
from datetime import date
import logging
from typing import Optional, List, Tuple

from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from dateutil.parser import parse as parse_date

from datafeeds.common.util.selenium import (
    ec_and,
    ec_or,
    element_text_doesnt_contain,
)
from datafeeds.common.util.pagestate.pagestate import PageState

import datafeeds.scrapers.sce_react.errors as sce_errors

log = logging.getLogger(__name__)

# A model of the basic usage info exposed for service accounts from the landing page
# of SCE's website
SimpleUsageInfo = collections.namedtuple(
    "UsageInfo", ["start_date", "end_date", "usage", "cost"]
)

# A model of the basic demand info exposed for service accounts from the landing page
# of SCE's website
SimpleDemandInfo = collections.namedtuple(
    "DemandInfo", ["start_date", "end_date", "demand", "cost"]
)

# A simple model of a service descriptor in the SCE Energy Manager UI. This corresponds to a row in a table of
# available services for a given login.
ServiceListingRow = collections.namedtuple(
    "ServiceListingRow",
    [
        "checkbox",  # For selecting an account for some Energy Manager report
        "row_id",
        "customer_number",
        "customer_name",
        "service_acct_id",
        "service_acct_name",
        "rate",  # Tariff
        "address",
        "bill_start",
        "bill_end",
    ],
)

BillingDataRow = collections.namedtuple(
    "BillingDataRow",
    [
        "selector",  # For selecting a billing row
        "service_acct",
        "service_acct_name",
        "service_acct_address",
        "bill_start_date",
        "bill_end_date",
        "statement_date",  # Tariff
        "bill_amount",
        "kw",
        "kwh",
    ],
)

GenericBusyIndicatorLocator = (
    By.XPATH,
    "//span[contains(@class, 'appSpinner__spinnerMessage')]",
)


def detect_and_close_survey(driver, timeout=5):
    try:
        locator = (By.CLASS_NAME, "acsDeclineButton")
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(locator)
        ).click()
        log.info("popup closed")
        driver.sleep(1)
    except Exception:
        pass


class SceLoginPage(PageState):
    """Page object for SCE Login

    This is a very straightforward login process, featuring a username field, password field, and submit button.
    """

    LoginLinkLocator = (By.XPATH, "//a[contains(@class, 'login-background]")
    UsernameInputLocator = (
        By.XPATH,
        "//div[contains(@class, 'LoginMainComponent')]  //input[" "@name='username']",
    )
    PasswordInputLocator = (
        By.XPATH,
        "//div[contains(@class, 'LoginMainComponent')] //input[" "@name='password']",
    )
    SubmitButtonLocator = (
        By.XPATH,
        "//div[contains(@class, 'LoginMainComponent')] //button["
        "contains(@class, 'sceBtnSmPrimary')]",
    )

    def get_ready_condition(self):
        return ec_and(
            EC.title_contains("Log In"),
            EC.presence_of_element_located(self.UsernameInputLocator),
            EC.presence_of_element_located(self.PasswordInputLocator),
            EC.presence_of_element_located(self.SubmitButtonLocator),
        )

    def login(self, username: str, password: str):
        username_field = self.driver.find_element(*self.UsernameInputLocator)
        username_field.send_keys(username)

        password_field = self.driver.find_element(*self.PasswordInputLocator)
        password_field.send_keys(password)

        submit_button = self.driver.find_element(*self.SubmitButtonLocator)
        submit_button.click()


class SceLoginFailedPage(PageState):
    """Page object representing a failed SCE login

    A failed login should produce an error message. This class facilitates detecting and retrieving that error message.
    """

    LoginErrorLocator = (By.XPATH, "//react-login-main//mark")

    def get_ready_condition(self):
        return ec_and(
            EC.title_contains("Log In"),
            EC.presence_of_element_located(self.LoginErrorLocator),
        )

    def raise_on_error(self):
        """Raise an exception describing the login error."""
        error = self.driver.find_element(*self.LoginErrorLocator)
        message = "Login failed. The website error is: '{}'".format(error.text)
        raise sce_errors.LoginFailedException(message)


class SceLandingPage(PageState):
    """Page object for the SCE Landing Page, automatically loaded after login.

    Note that several slightly different landing pages are possible on this website. However, this class is a
    "generic" landing page model; it attempts to capture all possible landing page states. There are more
    granular classes below that can detect different flavors of landing page (SceSingleAccountLandingPage,
    SceMultiAccountLandingPage, etc.)

    Sometimes, SCE fails to load the landing page, and shows an error message. This class can either tolerate
    that error, or not, by setting the tolerate_error field in the constructor. If this field is set to True,
    then this page recognizes the error page as a successful load. Else, if False, the error page is not
    recognized as a valid landing page state. This field has a default value of True.
    """

    BillingDataLocator = (
        By.XPATH,
        "//react-myaccount-container//div[contains(@class, 'billingOverviewComponent')]",
    )
    AccountDataLocator = (
        By.XPATH,
        "//react-myaccount-container//div[contains(@class, 'accountsOverviewComponent')]",
    )
    VerificationPendingLocator = (By.XPATH, "//react-verification-pending")
    ErrorLocator = (
        By.XPATH,
        "//react-myaccount-container//mark[contains(@class, 'globalErrorBlock')]",
    )

    def __init__(self, driver, tolerate_error=True):
        super().__init__(driver)
        self.tolerate_error = tolerate_error

    def get_ready_condition(self):
        locator_set = [
            EC.presence_of_element_located(self.BillingDataLocator),
            EC.presence_of_element_located(self.AccountDataLocator),
            EC.presence_of_element_located(self.VerificationPendingLocator),
        ]
        if self.tolerate_error:
            locator_set.append(EC.presence_of_element_located(self.ErrorLocator))
        return ec_or(*locator_set)


class SceSingleAccountLandingPage(PageState):
    """An SCE landing page showing information for a single account

    Some logins only display information for a single SAID on the landing page. This class models that flavor of
    landing page.
    """

    ViewUsageLocator = (
        By.XPATH,
        "//react-myaccount-container//button[@id='ThisPeriod']",
    )
    BillingDataLocator = (
        By.XPATH,
        "//react-myaccount-container//div[contains(@class, 'billingOverviewComponent')]",
    )
    ServiceAccountsLocator = (
        By.XPATH,
        "//div[contains(@class, 'serviceAccountComponent__sceServiceAccInfoSection')]",
    )

    def get_ready_condition(self):
        return ec_or(
            EC.presence_of_element_located(self.BillingDataLocator),
            EC.presence_of_element_located(self.ViewUsageLocator),
        )

    def get_service_account(self) -> str:
        """Extract the service account number listed on this landing page"""
        try:
            elements = self.driver.find_elements(*self.ServiceAccountsLocator)
        except NoSuchElementException:
            elements = None

        if not elements:
            raise sce_errors.ServiceIdException(
                "Failed to located any Service ID information on the SCE landing page."
            )
        elif len(elements) > 1:
            raise sce_errors.ServiceIdException(
                "Expected only one service account on the SCE landing page, but found multiple."
            )

        return elements[0].text.strip()

    def open_usage_info(self):
        """Open the usage info dialog for the available service agreement.

        This can be used to view billing data for the service account.
        """
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self.ViewUsageLocator)
            ).click()
        except Exception:
            raise sce_errors.BillingDataNotFoundException(
                "No billing data was found for this login."
            )


class SceMultiAccountLandingPage(PageState):
    """An SCE landing page showing information for multiple accounts

    Some logins display information for multiple SAIDS on the landing page. This class models that flavor of landing
    page. This page has a search bar that can be used to locate a specific service agreement.
    """

    AccountDataLocator = (
        By.XPATH,
        "//react-myaccount-container//div[contains(@class, 'accountsOverviewComponent')]",
    )
    AccountFilterLocator = (
        By.XPATH,
        "//react-myaccount-container//input[@id='searchMyAccounts']",
    )

    def get_ready_condition(self):
        return ec_or(
            EC.presence_of_element_located(self.AccountDataLocator),
            EC.presence_of_element_located(self.AccountFilterLocator),
        )

    def search_by_service_id(self, service_id):
        account_search_field = self.driver.find_element(*self.AccountFilterLocator)
        actions = ActionChains(self.driver)
        actions.move_to_element(account_search_field)
        actions.click(account_search_field)
        actions.send_keys_to_element(account_search_field, service_id)
        actions.send_keys_to_element(account_search_field, Keys.ENTER)
        actions.perform()


class SceAccountSearchFailure(PageState):
    """Models the case where a SAID search fails"""

    ErrorLocator = (
        By.XPATH,
        "//react-myaccount-container//div[contains(@class, 'sceErrorBox')]",
    )

    def get_ready_condition(self):
        return EC.presence_of_element_located(self.ErrorLocator)


class SceAccountSearchSuccess(PageState):
    """Models the case where an SAID search succeeds"""

    AccountDataLocator = (
        By.XPATH,
        "//react-myaccount-container//div[contains(@class, 'serviceAccOverviewComponent__sceServiceAccSection')]",
    )

    ViewUsageLinkLocator = (
        By.XPATH,
        "//a[contains(@class, 'serviceAccOverviewComponent__sceViewUsageBtn')]",
    )

    def get_ready_condition(self):
        return ec_and(
            EC.presence_of_element_located(self.AccountDataLocator),
            EC.presence_of_element_located(self.ViewUsageLinkLocator),
        )

    def view_usage_for_search_result(self):
        """Open the usage info dialog for the available service agreement.

        This can be used to view billing data for the service account.
        """
        self.driver.find_element(*self.ViewUsageLinkLocator).click()


class SceServiceAccountDetailModal(PageState):
    """Models the modal dialog that appears upon clicking "View Usage" for a service agreement

    This dialog is somewhat complex; usage and demand values are displayed in two separate reports. Available billing
    dates are available from a somewhat messy calendar view. This class tries to hide most of that complexity behind
    two methods: get_usage_info and get_demand_info. These methods accept a date range, and return applicable
    use/demand values for bills that fall in that range (according to the starting date of the bill).
    """

    ReportWindow = (By.ID, "graphModal")
    ReportDropdownLocator = (By.ID, "OpenDropDown")
    OpenDateRangeLocator = (By.ID, "showDateRanges")
    DatePopupLocator = (
        By.XPATH,
        "//div[contains(@class, 'GraphDialogs__dateRangePopUp')]",
    )
    DatePopupYearLocator = (
        By.XPATH,
        "//div[contains(@class, 'GraphDialogs__dateRangePopUp')]//p",
    )

    def get_ready_condition(self):
        return ec_and(
            EC.presence_of_element_located(self.ReportWindow),
            EC.presence_of_element_located(self.ReportDropdownLocator),
            EC.invisibility_of_element_located(GenericBusyIndicatorLocator),
        )

    def _select_report(self, name: str, locator):
        """Use to select a specific report type (often demand or usage)

        Arguments:
            name: The display-name name of the report (e.g. "View Usage")
            locator: The selenium locator for the report in the website UI dropdown
        """
        report_dropdown = self.driver.find_element(*self.ReportDropdownLocator)
        current_report = report_dropdown.text.strip()
        if current_report == name:
            return
        report_dropdown.click()
        self.driver.find_element(*locator).click()
        time.sleep(5)
        WebDriverWait(self.driver, 20).until(
            EC.invisibility_of_element_located(GenericBusyIndicatorLocator)
        )

    def select_usage_report(self):
        self._select_report("View Usage", (By.ID, "ViewUsage"))

    def select_demand_report(self):
        self._select_report("View Demand info", (By.ID, "ViewDemand"))

    def _parse_date_range(self, text) -> Tuple[date, date]:
        """Attempt to parse a date range string from the calendar view"""
        try:
            parts = text.split("-")
            return parse_date(parts[0]).date(), parse_date(parts[1]).date()
        except Exception as e:
            raise ValueError(
                "Failed to parse date range string: {}".format(text)
            ) from e

    def _parse_usage(self, text) -> float:
        """Attempt to convert a string usage value from the UI to a float"""
        text = text.lower().replace("kwh", "").replace(",", "")
        return float(text)

    def _parse_demand(self, text) -> Optional[float]:
        """Attempt to convert a string demand value from the UI to a float.

        Sometimes this is reported as "NaN" (presumably, "Not a Number") in the SCE UI. In this case, we return None.
        """
        text = text.lower().replace("kw", "").replace(",", "")
        if "nan" in text:
            return None
        return float(text)

    def _parse_cost(self, text):
        text = text.lower().replace("$", "").replace(",", "")
        return float(text)

    def _get_visible_date_range_elements(self) -> List[WebElement]:
        """Internal helper function to retrieve currently visible billing period date ranges from the calendar view.

        This returns the raw selenium elements holding potential date ranges."""

        # This is a little fragile; the selector is complex, and the exclusion of a specific string is a little
        # questionable. This might need to be replaced by something more robust if it proves troublesome.
        date_divs_locator = (
            By.XPATH,
            "//div[contains(@class, 'GraphDialogs__dateRangePopUp')]/div/div//div",
        )
        return [
            element
            for element in self.driver.find_elements(*date_divs_locator)
            if "View Another Billed Month" not in element.text
        ]

    def _get_visible_date_ranges(self) -> List[Tuple[date, date]]:
        """Internal helper function to retrieve currently visible billing period date ranges from the calendar view.

        This converts the raw selenium elements into datetime.date tuples of the form (start, end)"""
        result = []
        for date_elem in self._get_visible_date_range_elements():
            date_text = date_elem.text.strip()
            start, end = self._parse_date_range(date_text)
            result.append((start, end))
        return result

    def get_available_billing_periods(self) -> List[Tuple[date, date]]:
        """Produce a list of all available billing date ranges from the calendar view in the modal dialog."""

        results = []
        self.driver.find_element(*self.OpenDateRangeLocator).click()
        date_popup = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(self.DatePopupLocator)
        )

        while True:
            current_year = self.driver.find_element(*self.DatePopupYearLocator).text
            for start, end in self._get_visible_date_ranges():
                results.append((start, end))

            prev_button = date_popup.find_element_by_xpath(
                ".//span[contains(@class, GraphDialogs__prevIcon)]"
            )
            prev_button.click()

            try:
                WebDriverWait(self.driver, 5).until(
                    element_text_doesnt_contain(self.DatePopupYearLocator, current_year)
                )
            except TimeoutException:
                break

        self.driver.find_element_by_xpath("//button[@id='showDateRanges']").click()
        WebDriverWait(self.driver, 5).until(
            EC.invisibility_of_element_located(self.DatePopupLocator)
        )
        return results

    def _get_visible_usage_info(self):
        """Internal helper function to retrieve currently visible usage info from the modal dialog"""
        data_xpath = (
            "//div[contains(@class, 'GraphContentStyle__netUsageValues')]"
            "//span[contains(@class, 'GraphContentStyle__netSuperOffPeakKwh')]"
        )

        values = self.driver.find_elements_by_xpath(data_xpath)
        if len(values) != 3:
            raise ValueError(
                "Did not find expected usage information on the SCE website"
            )

        # Skip the first data element (values[0]), which is average daily usage
        usage = self._parse_usage(values[1].text)
        cost = self._parse_cost(values[2].text)
        return usage, cost

    def get_visible_demand_info(self):
        """Internal helper function to retrieve currently visible demand info from the modal dialog"""
        data_xpath = (
            "//div[contains(@class, 'GraphContentStyle__netUsageValues')]"
            "//span[contains(@class, 'GraphContentStyle__netSuperOffPeakKwh')]"
        )

        values = self.driver.find_elements_by_xpath(data_xpath)
        if len(values) != 2:
            raise ValueError(
                "Did not find expected demand information on the SCE website"
            )

        demand = self._parse_demand(values[0].text)
        cost = self._parse_cost(values[1].text)
        return demand, cost

    def select_date_range(self, target_start: date, target_end: date):
        """Attempt to select a specific billing period from the modal dialog calendar view"""
        self.driver.find_element(*self.OpenDateRangeLocator).click()
        date_popup = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(self.DatePopupLocator)
        )

        found_date = False
        while True:
            current_year = self.driver.find_element(*self.DatePopupYearLocator).text
            for date_elem in self._get_visible_date_range_elements():
                date_text = date_elem.text.strip()
                if "View Another Billed Month" in date_text:
                    continue

                start, end = self._parse_date_range(date_text)
                if start == target_start and end == target_end:
                    found_date = True
                    date_elem.click()
                    break

            if found_date:
                break

            prev_button = date_popup.find_element_by_xpath(
                ".//span[contains(@class, GraphDialogs__prevIcon)]"
            )
            prev_button.click()

            try:
                WebDriverWait(self.driver, 5).until(
                    element_text_doesnt_contain(self.DatePopupYearLocator, current_year)
                )
            except TimeoutException:
                break

        if not found_date:
            raise ValueError(
                "Failed to find bill with date range: {} - {}".format(
                    target_start, target_end
                )
            )

        # Wait for the selection to finish
        time.sleep(5)
        WebDriverWait(self.driver, 20).until(
            ec_and(
                EC.invisibility_of_element_located(self.DatePopupLocator),
                EC.invisibility_of_element_located(GenericBusyIndicatorLocator),
            )
        )

    def get_usage_info(self, start_date: date, end_date: date) -> List[SimpleUsageInfo]:
        """Scrape basic usage data for all billing periods that start in the range specified by the arguments

        Specifically all billing periods such that: start_date <= billing_period_start <= end_date
        """

        # Ensure we are looking at usage data
        self.select_usage_report()

        # We need to open the "Billed Months" view, in order to view historical data
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "BilledMonths"))
        ).click()
        time.sleep(5)
        WebDriverWait(self.driver, 20).until(
            EC.invisibility_of_element_located(GenericBusyIndicatorLocator)
        )

        # Get the available billing dates
        date_ranges = self.get_available_billing_periods()

        # Scrape usage info for applicable billing periods
        results = []
        for start, end in date_ranges:
            if start_date <= start <= end_date:
                self.select_date_range(start, end)
                usage, cost = self._get_visible_usage_info()
                results.append(
                    SimpleUsageInfo(
                        start_date=start, end_date=end, usage=usage, cost=cost
                    )
                )
        return results

    def get_demand_info(self, start_date, end_date) -> List[SimpleDemandInfo]:
        """Scrape basic demand data for all billing periods that start in the range specified by the arguments

        Specifically all billing periods such that: start_date <= billing_period_start <= end_date
        """

        # Ensure we are looking at demand data
        self.select_demand_report()

        # We need to open the "Billed Months" view, in order to view historical data
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "demandBilledMonths"))
        ).click()
        time.sleep(5)
        WebDriverWait(self.driver, 20).until(
            EC.invisibility_of_element_located(GenericBusyIndicatorLocator)
        )

        # Get the available billing dates
        date_ranges = self.get_available_billing_periods()

        # Scrape demand info for applicable billing periods
        results = []
        for start, end in date_ranges:
            if start_date <= start <= end_date:
                self.select_date_range(start, end)
                demand, cost = self.get_visible_demand_info()
                results.append(
                    SimpleDemandInfo(
                        start_date=start, end_date=end, demand=demand, cost=cost
                    )
                )
        return results


class SceEnergyManagerLandingPage(PageState):
    """Page object for the Energy Manager landing page

    This models the UI state upon initially navigating to the Energy Manager tab on the SCE website. The only action
    supported in this state is selecting a specific Energy Manager report type.
    """

    ReportTypeLocator = (By.XPATH, "//react-energy-manager//div[@id='emReportType']")

    def get_ready_condition(self):
        return EC.presence_of_element_located(self.ReportTypeLocator)

    def select_report(self, name: str):
        report_type_element = self.driver.find_element(*self.ReportTypeLocator)
        current_report_type = report_type_element.text.strip()
        if current_report_type == name:
            return

        report_type_element.click()

        try:
            report_selector = "//ul[@id='emReportTypeUl']//li[text()='{}']".format(name)
            self.driver.find_element_by_xpath(report_selector).click()
        except NoSuchElementException:
            raise ValueError("Could not find a report type named: {}".format(name))

    def select_basic_usage_report(self):
        """Navigate to the Basic Usage report type. We can use this report to retrieve interval data."""
        self.select_report("Basic Usage")

    def select_billing_report(self):
        """Navigate to the Service Acct Bill List report type. We can use this report to retrieve billing data."""
        self.select_report("Service Acct Bill List")


class EnergyManagerServiceListingHelper:
    """A helper class for interacting with the Energy Manager service account selection UI

    The same service account selection UI is shared by various Energy Manager report types. This class features some
    simple functions for interacting with this UI, primarily for selecting a given service ID.
    """

    ViewSmartConnectLocator = (By.PARTIAL_LINK_TEXT, "View Edison SmartConnect")
    ServiceTableLocator = (
        By.XPATH,
        "//react-energy-manager//table[contains(@class, 'emUserData')]",
    )
    ServiceDivLocator = (
        By.XPATH,
        "//react-energy-manager//div[contains(@class, 'emUserData__divscroll')]",
    )
    EnergyManagerErrorLocator = (
        By.XPATH,
        "//react-energy-manager//div[contains(@class, 'errorpage__applyborder')]",
    )
    TableColumnCount = 9

    def __init__(self, driver):
        self.driver = driver

    def parse_service_row(self, row: WebElement) -> Optional[ServiceListingRow]:
        """Parse account information out of an HTML table row."""
        data = row.find_elements_by_tag_name("td")
        if len(data) != self.TableColumnCount:
            return None
        log.info("found service id %s", data[3].text)

        return ServiceListingRow(
            checkbox=data[0].find_element_by_xpath(
                ".//div[contains(@class, 'sce-registration-checkbox')]//..//.."
            ),
            row_id=data[0].find_element_by_tag_name("input").get_attribute("id"),
            customer_number=data[1].text,
            customer_name=data[2].text,
            service_acct_id=data[3].text,
            service_acct_name=data[4].text,
            rate=data[5].text,
            address=data[6].text,
            bill_start=data[7].text,
            bill_end=data[8].text,
        )

    def select_service_id_in_div(
        self, service_id: str, service_div: WebElement
    ) -> Optional[ServiceListingRow]:
        """Helper function for service ID selection.

        There are two separate tables from which one can select services; one for Edison Smart connect meters,
        and one for normal meters. These tables live in separate HTML divs. This function generalizes the
        activity of selecting an account within one of those divs.

        The service account tables are paginated, so this function takes care of going through the pages.
        """

        while True:
            # Parse service information out of the table
            table_rows = service_div.find_elements_by_tag_name("tr")
            parsed_rows = []
            for row in table_rows:
                parsed_row = self.parse_service_row(row)
                if parsed_row:
                    parsed_rows.append(parsed_row)

            if not parsed_rows:
                break

            # Look for the desired service ID
            for parsed_row in parsed_rows:
                if service_id == parsed_row.service_acct_id:
                    return parsed_row

            try:
                next_button = service_div.find_element_by_partial_link_text("Next")
            except NoSuchElementException:
                break
            next_button.click()

            # Currently, we just sleep a few seconds to wait for the page to change. These changes are pretty fast,
            # in general, so there isn't a huge risk of accessing stale elements. It's a little tricky to write logic
            # to detect the DOM updates related to the page changing, but if this wait proves problematic we can
            # revisit that.
            time.sleep(10)

        return None

    def get_matching_service_row(self, service_id: str) -> Optional[ServiceListingRow]:
        """Attempt to return the service row with the matching service ID in the Energy Manager UI.

        Returns the service row if the service ID was successfully found, None otherwise.
        Note that this function is currently not idempotent; it can't be called twice
        with different service IDs, for example. It expects the Energy Manager page to be
        in its initial configuration, and causes various DOM modifications as part of selecting
        the desired service.
        """
        log.debug("looking for service_id %s", service_id)
        service_divs = self.driver.find_elements(*self.ServiceDivLocator)
        if not service_divs:
            return None

        service_row = self.select_service_id_in_div(service_id, service_divs[0])
        if service_row:
            return service_row

        if self.try_expand_smart_meters():
            # A new div will appear with the smart meters, so requery the DOM
            service_divs = self.driver.find_elements(*self.ServiceDivLocator)
            if len(service_divs) > 1:
                service_row = self.select_service_id_in_div(service_id, service_divs[1])
                return service_row

        return None

    def try_expand_smart_meters(self):
        """Attempt to expand the Edison Smart Meters portion of the Energy Manager UI.

        These are additional services that by default are hidden.
        """

        # We define a helper function to help detect when the Smart Connect meters are visible in the UI
        def smart_meters_visible(driver):
            try:
                service_divs = driver.find_elements(*self.ServiceDivLocator)
                if len(service_divs) != 2:
                    return False
                table_rows = service_divs[1].find_elements_by_tag_name("tr")
                for row in table_rows:
                    parsed_row = self.parse_service_row(row)
                    if parsed_row:
                        return True
            except Exception:
                pass
            return False

        # The link to expand smart meters might not be present, for some logins
        try:
            link = self.driver.find_element(*self.ViewSmartConnectLocator)
        except NoSuchElementException:
            return False

        # This is some "wisdom" from the previous scraper, might not be necessary in the new UI
        if not link.is_displayed():
            return False

        # Finally, try to click the link
        link.click()
        try:
            WebDriverWait(self.driver, 10).until(smart_meters_visible)
        except TimeoutException:
            return False

        return True


class SceEnergyManagerBasicUsagePage(PageState):
    """Page object for the Basic Usage UI in Energy Manager

    This interface is used to extract interval data from the SCE website.
    """

    ReportTypeLocator = (By.XPATH, "//react-energy-manager//div[@id='emReportType']")

    TimePeriodLocator = (By.XPATH, "//div[@id='uday']")
    CustomTimeLocator = (By.XPATH, "//li[@id='udayCUS']")
    FromDateLocator = (By.XPATH, "//input[@id='EMStartDate']")
    ToDateLocator = (By.XPATH, "//input[@id='EMEndToDate']")

    ReportUnitsLocator = (
        By.XPATH,
        "//div[contains(@class, 'reportType__checkboxReport')]//input",
    )
    ReportDataKindLocator = (By.XPATH, "//div[@id='includeShow']")
    DataKindMeteredLocator = (By.XPATH, "//li[@id='includeShowME']")

    GenerateReportLocator = (By.XPATH, "//button[@id='emGenerateReport']")
    DownloadExcelLocator = (By.XPATH, "//a[@id='generatedReportExcel']")
    ErrorLocator = (By.XPATH, "//div[contains(@class, 'reportType__error')]//mark")

    def get_ready_condition(self):

        # Helper function to check that the correct report type is selected
        def basic_usage_selected(driver):
            try:
                report_type_element = driver.find_element(*self.ReportTypeLocator)
                current_report_type = report_type_element.text.strip()
                return current_report_type == "Basic Usage"
            except NoSuchElementException:
                pass
            return False

        return basic_usage_selected

    def select_service_id(self, service_id: str) -> ServiceListingRow:
        """Choose a specific service ID to gather data for"""
        service_listing = EnergyManagerServiceListingHelper(self.driver)
        service_row = service_listing.get_matching_service_row(service_id)
        if not service_row:
            message = "No service ID matching '{}' was found in Energy Manager".format(
                service_id
            )
            raise sce_errors.ServiceIdException(message)
        time.sleep(5)
        log.debug("trying to click checkbox")
        service_row.checkbox.click()
        return service_row

    def configure_report(self):
        """Apply a default configuration to the report

        Ensures that we are gathering 15 minute data with KW units, with custom date range.
        """

        # Specify a custom date range, so that we can select the dates we need
        self.driver.find_element(*self.TimePeriodLocator).click()
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(self.CustomTimeLocator)
        ).click()

        # Specify units of KW
        unit_options = self.driver.find_elements(*self.ReportUnitsLocator)
        for unit_option in unit_options:
            if unit_option.get_attribute("id") == "KW":
                if not unit_option.is_selected():
                    unit_option.click()
            else:
                if unit_option.is_selected():
                    unit_option.click()

        # Specify that we want "Metered" data, which typically should be 15 minute granularity.
        data_kind = self.driver.find_element(*self.ReportDataKindLocator)
        if data_kind.text != "Metered":
            data_kind.click()
            WebDriverWait(self.driver, 10).until(
                EC.visibility_of_element_located(self.DataKindMeteredLocator)
            ).click()

    def set_time_range(self, start_date: date, end_date: date):
        date_fmt = "%m/%d/%Y"
        from_input = self.driver.find_element(*self.FromDateLocator)
        to_input = self.driver.find_element(*self.ToDateLocator)

        from_str = start_date.strftime(date_fmt)
        to_str = end_date.strftime(date_fmt)

        # The code for updating the date text fields is a little funny looking right now. It seemed like the normal
        # selenium methods for clearing a textbox weren't working at the time of writing (e.g. the clear() function),
        # so we manually issue backspace keys to clear out these text fields.

        actions = ActionChains(self.driver)
        actions.click(to_input)
        actions.pause(2)
        for _ in range(20):
            actions.send_keys_to_element(to_input, Keys.BACK_SPACE)
        actions.pause(2)
        actions.perform()
        log.debug("cleared to_input")
        to_input.send_keys(to_str)
        log.debug("sent %s to to_input", to_str)

        actions = ActionChains(self.driver)
        actions.click(from_input)
        actions.pause(2)
        for _ in range(20):
            actions.send_keys_to_element(from_input, Keys.BACK_SPACE)
        actions.pause(2)
        actions.perform()
        log.debug("cleared from_input")
        from_input.send_keys(from_str)
        log.debug("sent %s to from_input", from_str)

    def generate_report(self):
        """Start a report generation task"""

        # This was used before to help build a wait condition, but is probably more complicated than necessary,
        # Preserving in a comment for now in case it useful later.
        # report_details_locator = (By.XPATH, "//div[contains(@class, 'reportType__generatedReport')]/div/div[1]")
        # current_report_details = None
        # try:
        #   report_details_elem = self.driver.find_element(*report_details_locator)
        #   current_report_details = report_details_elem.text.strip()
        # except:
        #    pass

        self.driver.find_element(*self.GenerateReportLocator).click()

    def get_report_error_element(self) -> Optional[WebElement]:
        """Return the WebElement containing report errors, if it exists (None otherwise)"""
        try:
            return WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self.ErrorLocator)
            )
        except TimeoutException:
            return None

    def raise_on_report_error(self):
        """Raise a report generation error as an exception"""
        error = self.get_report_error_element()
        if not error:
            return

        error_text = error.text.strip()
        if error_text.startswith("Data not found for the selected time period"):
            raise sce_errors.EnergyManagerDataNotFoundException(error.text)
        else:
            msg = "Energy Manager encountered an error: '{}'".format(error.text)
            raise sce_errors.EnergyManagerReportException(msg)

    def download_report(self):
        """Initial a download of the interval data report as a CSV file"""

        # For some reason, we seem to get a popup showing up at this particular moment in a number of tests
        # To protect against that, try this a couple of times, if it fails, closing the popup in between
        retries = 2
        while True:
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.visibility_of_element_located(self.DownloadExcelLocator)
                ).click()
            except Exception as exc:
                log.info("click download failed: %s; %s tries", exc, retries)
                detect_and_close_survey(self.driver)
                if retries == 0:
                    raise exc
                retries -= 1
            else:
                break


class SceEnergyManagerBillingPage(PageState):
    """Page object for the "Service Acct Bills" tool in Energy Manager

    This interface is used to extract billing data from the SCE website.
    """

    ReportTypeLocator = (By.XPATH, "//react-energy-manager//div[@id='emReportType']")
    TimePeriodLocator = (By.XPATH, "//div[@id='timePeriodDropDowns']")
    CustomTimeLocator = (By.XPATH, "//li[@id='timePeriodDropDownsCUST']")
    FromMonthLocator = (By.XPATH, "//div[@id='monthOneDropdown']")
    FromYearLocator = (By.XPATH, "//div[@id='yearOneDropdown']")
    ToMonthLocator = (By.XPATH, "//div[@id='monthTwoDropdown']")
    ToYearLocator = (By.XPATH, "//div[@id='yearTwoDropdown']")
    GenerateReportLocator = (By.XPATH, "//button[@id='emGenerateReport']")
    BillingTableLocator = (
        By.XPATH,
        "//div[contains(@class,'ServiceAcctBillList__module')]//table",
    )
    ErrorLocator = (By.XPATH, "//div[contains(@class, 'reportType__error')]//mark")

    def get_ready_condition(self):

        # Helper function to check that the correct report type is selected
        def billing_report_selected(driver):
            try:
                report_type_element = driver.find_element(*self.ReportTypeLocator)
                current_report_type = report_type_element.text.strip()
                return current_report_type == "Service Acct Bill List"
            except NoSuchElementException:
                pass
            return False

        return ec_and(
            billing_report_selected,
            EC.presence_of_element_located(self.TimePeriodLocator),
        )

    def configure_report(self):
        """Apply a simple configuration to the report"""

        # Specify a custom date range, so that we can select the dates we need
        self.driver.find_element(*self.TimePeriodLocator).click()
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(self.CustomTimeLocator)
        ).click()

    def select_service_id(self, service_id: str) -> ServiceListingRow:
        """Choose a specific service ID to gather data for"""
        service_listing = EnergyManagerServiceListingHelper(self.driver)
        service_row = service_listing.get_matching_service_row(service_id)
        if not service_row:
            message = "No service ID matching '{}' was found in Energy Manager".format(
                service_id
            )
            raise sce_errors.ServiceIdException(message)
        time.sleep(5)
        log.debug("trying to click checkbox")
        service_row.checkbox.click()
        return service_row

    def set_time_range(self, start_date: date, end_date: date):
        # Set the "From" month
        self.driver.find_element(*self.FromMonthLocator).click()
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(
                (By.ID, "monthOneDropdown{}".format(start_date.month))
            )
        ).click()

        # Set the "From" year
        self.driver.find_element(*self.FromYearLocator).click()
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(
                (By.ID, "yearOneDropdown{}".format(start_date.year))
            )
        ).click()

        # Set the "To" month
        self.driver.find_element(*self.ToMonthLocator).click()
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(
                (By.ID, "monthTwoDropdown{}".format(end_date.month))
            )
        ).click()

        # Set the "To" year
        self.driver.find_element(*self.ToYearLocator).click()
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(
                (By.ID, "yearTwoDropdown{}".format(end_date.year))
            )
        ).click()

    def generate_report(self):
        """Start a report generation task"""
        self.driver.find_element(*self.GenerateReportLocator).click()

    def get_report_error_element(self) -> Optional[WebElement]:
        """Return the WebElement containing report errors, if it exists (None otherwise)"""
        try:
            return WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self.ErrorLocator)
            )
        except TimeoutException:
            return None

    def raise_on_report_error(self):
        """Raise a report generation error as an exception"""
        error = self.get_report_error_element()
        if not error:
            return

        error_text = error.text.strip()
        if error_text.startswith("Data not found for the selected time period"):
            raise sce_errors.EnergyManagerDataNotFoundException(error.text)
        else:
            msg = "Energy Manager encountered an error: '{}'".format(error.text)
            raise sce_errors.EnergyManagerReportException(msg)

    def _parse_amount(self, text: str) -> float:
        """Attempt to convert a string cost value from the energy manager UI to a float"""
        text = text.lower().replace("$", "").replace(",", "")
        return float(text)

    def _parse_usage(self, text) -> float:
        """Attempt to convert a string usage value from the energy manager UI to a float"""
        text = text.replace(",", "")
        return float(text)

    def _parse_demand(self, text) -> float:
        """Attempt to convert a string demand value from the energy manager UI to a float"""
        text = text.replace(",", "")
        return float(text)

    def _parse_billing_row(self, row: WebElement) -> BillingDataRow:
        """Parse an HTML <tr> element containing billing data from the Energy Manager UI"""

        expected_columns = 10
        data_elements = row.find_elements_by_tag_name("td")
        if len(data_elements) != expected_columns:
            msg = "Unexpected row found in the billing data table. The raw row text is: {}".format(
                row.text
            )
            raise sce_errors.BillingDataParseException(msg)
        try:
            return BillingDataRow(
                selector=data_elements[0].find_elements_by_tag_name("input")[0],
                service_acct=data_elements[1].text,
                service_acct_name=data_elements[2].text,
                service_acct_address=data_elements[3].text,
                bill_start_date=parse_date(data_elements[4].text),
                bill_end_date=parse_date(data_elements[5].text),
                statement_date=parse_date(data_elements[6].text),
                bill_amount=self._parse_amount(data_elements[7].text),
                kw=self._parse_demand(data_elements[8].text),
                kwh=self._parse_usage(data_elements[9].text),
            )
        except Exception as e:
            msg = "Failed to parse a row of billing data from the SCE website. The raw row text is: {}".format(
                row.text
            )
            raise sce_errors.BillingDataParseException(msg) from e

    def get_visible_billing_data(self) -> List[BillingDataRow]:
        """Extract the visible billing data from the Energy Manager UI.

        This should be called after generating a report to scrape the resulting bills.
        """
        billing_table = self.driver.find_element(*self.BillingTableLocator)
        table_rows = billing_table.find_elements_by_tag_name("tr")
        first_row = True
        results = []
        for row in table_rows:
            # Skip over the header
            if first_row:
                first_row = False
                continue
            billing_data = self._parse_billing_row(row)
            results.append(billing_data)
        return results

    def get_minimum_selectable_start_date(self) -> date:
        """Determine the minimum possible selectable start date in the Energy Manager billing UI"""
        from_year_dropdown = self.driver.find_element_by_id("yearOneDropdownUl")
        year_elements = from_year_dropdown.find_elements_by_tag_name("li")
        available_years = []

        for elem in year_elements[1:]:
            value_attr = elem.get_attribute("value")
            if value_attr:
                available_years.append(int(value_attr))

        # Any month can be selected for a given year, so the minimum selectable date is January
        return date(year=min(available_years), month=1, day=1)
