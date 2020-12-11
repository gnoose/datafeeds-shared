import re
import time
import os
import logging

from io import BytesIO
from typing import Optional, List

from dateutil.parser import parse as parse_date
from datetime import date, timedelta
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select

from datafeeds import config, db
from datafeeds.common.alert import post_slack_message
from datafeeds.common.batch import run_datafeed
from datafeeds.common.captcha import recaptcha_v2
from datafeeds.common.support import Results
from datafeeds.common.base import BaseWebScraper, CSSSelectorBasePageObject
from datafeeds.common.support import Configuration
from datafeeds.common.typing import Status, BillingDatum
from datafeeds.common.upload import upload_bill_to_s3, hash_bill

from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
    UtilityService,
    SnapmeterAccountMeter,
)

log = logging.getLogger(__name__)


def notify_rebill(meter_number: str, statement: date):
    meter = (
        db.session.query(Meter)
        .filter(
            UtilityService.service_id == meter_number,
            Meter.service == UtilityService.oid,
        )
        .first()
    )
    account = (
        db.session.query(SnapmeterAccount)
        .filter(
            UtilityService.service_id == meter_number,
            Meter.service == UtilityService.oid,
            Meter.oid == SnapmeterAccountMeter.meter,
            SnapmeterAccountMeter.account == SnapmeterAccount.oid,
        )
        .first()
    )
    if meter and account:
        message = "%s (%s) in %s, statement date %s" % (
            meter.name,
            meter_number,
            account.name,
            statement.strftime("%m/%d/%y"),
        )
    else:
        message = "meter number %s, statement date %s" % (
            meter_number,
            statement.strftime("%m/%d/%y"),
        )
    post_slack_message(
        "LAWDP PDF scraper found a bill with corrections: %s" % message,
        "#scrapers",
        ":exclamation:",
        username="Scraper monitor",
    )


def get_pdf_text(filename) -> str:
    lines: List[str] = []
    for page in [p for p in extract_pages(filename)]:
        elements = [el for el in page if isinstance(el, LTTextContainer)]
        for element in elements:
            line = element.get_text().replace("\n", " ").strip()
            # modify METER NUMBER line to make searching easier ( remove space in between )
            if re.match(r"METER NUMBER  (\w+-\d+)  (\d+)", line):
                _meter_num = re.search(r"METER NUMBER  (\w+-\d+)  (\d+)", line).group(
                    1, 2
                )
                line = "METER NUMBER  {}{}".format(_meter_num[0], _meter_num[1])
            lines.append(line)

    pdf_text = "\n".join(lines)
    text_filename = filename.replace(r".pdf", r".txt")
    with open("%s" % text_filename, "w") as f:
        f.write(pdf_text)
        log.info("wrote text to %s" % text_filename)
    return pdf_text


def parse_ccf_bill(meter_number: str, pdf_text: str) -> List[BillingDatum]:
    """Method for parsing Water and Fire Bills"""
    bills: List[BillingDatum] = []
    regexes = {
        "bill_date": r"BILL DATE  (.+)",
        "meter_number": fr"METER NUMBER  (.+)  \d+",
        "billing_period": r"BILLING PERIOD  (?:\d+/\d+/\d+) - (?:\d+/\d+/\d+)",
        "water_billing_section": fr"SA # : {meter_number}[\s\S]+?Total Water Charges",
        "sub_billing_period": (
            r"BILLING PERIOD  (?:\d+\/\d+\/\d+) - (?:\d+\/\d+\/\d+)[\s\S]+?"
            r"State Energy Surcharge - \d+ days\n(.+?kWh)[\s\S]+?"
            r"Electric Charges (\d+/\d+/\d+) - (\d+/\d+/\d+) \(\d+ Days\)[\s\S]+?"
            r"\$([\d,\.]+)[\s\S]+?"
            r"\$([\d,\.]+)"
        ),
        "sub_bill_data": (
            r"State Energy Surcharge - \d+ days\n(?P<used>.+?)kWh[\s\S]+?"
            r"Electric Charges (?P<start_date>\d+/\d+/\d+) - (?P<end_date>\d+/\d+/\d+) \(\d+ Days\)[\s\S]+?"
            r"\$(?:[\d,\.]+)[\s\S]+?"
            r"\$(?P<cost>[\d,\.]+)"
        ),
        "fire_service_data": (
            r"Fire Service Charges\n"
            rf"SA # : {meter_number}[\s\S]+?"
            r"BILLING PERIOD  (?P<start_date>[\d\/]+) - (?P<end_date>[\d\/]+)[\s\S]+?"
            r"^(?P<used>[\d\.]+) HCF$[\s\S]+?"
            r"Total Fire Service Charges  \$ (?P<cost>[\d\.]+)"
        ),
    }

    bill_date_str = re.search(regexes["bill_date"], pdf_text).group(1)
    bill_date = parse_date(bill_date_str).date()
    bill_data_section_match = re.search(regexes["water_billing_section"], pdf_text)

    if not bill_data_section_match:
        # check if we have a Fire Service bill and parse that
        fire_data_match = re.search(
            regexes["fire_service_data"], pdf_text, re.MULTILINE
        )

        if fire_data_match:
            bills.append(
                BillingDatum(
                    start=parse_date(fire_data_match.group("start_date")).date(),
                    end=parse_date(fire_data_match.group("end_date")).date(),
                    statement=bill_date,
                    cost=float(fire_data_match.group("cost")),
                    used=float(fire_data_match.group("used")),
                    peak=None,
                    attachments=None,
                    utility_code=None,
                    items=None,
                )
            )
        else:
            log.warning("couldn't find Water or Fire Service Charges in pdf_text")
    else:
        # Parse water bill
        bill_data_section_text = bill_data_section_match.group()
        billing_periods = re.findall(regexes["billing_period"], bill_data_section_text)
        # Check if we have multiple billing periods in a bill
        if len(billing_periods) > 1:
            # We're only interested in the "sub" billing periods, delete the first billing
            # period line from the text, so that regexes["billing_period"] doesn't match it.
            bill_data_section_text = re.sub(
                regexes["billing_period"], "", bill_data_section_text, count=1,
            )
            bill_data_subsections = re.split(
                regexes["billing_period"], bill_data_section_text
            )
            # pop irrelevant lines above the first billing_period match
            bill_data_subsections.pop(0)
            for bill_data_subsection in bill_data_subsections:
                # bill: Dict[str, Any] = dict(start=None, end=None, statement=bill_date, cost=None, used=None, peak=None)
                dates_match = re.search(
                    r"Water Charges (\d+\/\d+\/\d+) - (\d+\/\d+\/\d+) \(\d+ Days\)",
                    bill_data_subsection,
                )
                cost_match = re.findall(
                    r"([\d\.]+) HCF x \$([\d\.]+)\/HCF", bill_data_subsection
                )

                if cost_match and dates_match:
                    bills.append(
                        BillingDatum(
                            start=parse_date(dates_match.group(1)).date(),
                            end=parse_date(dates_match.group(2)).date(),
                            statement=bill_date,
                            cost=round(
                                sum([float(x[0]) * float(x[1]) for x in cost_match]), 2
                            ),
                            used=round(sum([float(x[0]) for x in cost_match]), 5),
                            peak=None,
                            attachments=None,
                            utility_code=None,
                            items=None,
                        )
                    )
                else:
                    log.warning(
                        "Couldn't extract cost or start/end dates from water bill"
                    )
        else:
            raise Exception(
                "Multiple billing periods not yet implemented for ccf bills"
            )

    return bills


def parse_pdf(filename: str, meter_number: str, commodity: str) -> List[BillingDatum]:
    """Parse a PDF and return a list of BillingDatum objects, sorted by start date."""

    pdf_text: str = get_pdf_text(filename)

    bills: List[BillingDatum] = []

    regexes = {
        "bill_date": r"BILL DATE  (.+)",
        "meter_number": fr"METER NUMBER  (.+)  \d+",
        "billing_period": r"BILLING PERIOD  (\d+/\d+/\d+) - (\d+/\d+/\d+)",
        "billing_section": (
            r"(?:[\s\S]*)"  # This ensures that we only match the BILLING PERIOD occurrence that is closest to METER NUMBER
            r"(BILLING PERIOD  (?:\d+\/\d+\/\d+) - (?:\d+\/\d+\/\d+)[\s\S]+"
            fr"METER NUMBER  {meter_number}[\s\S]+?"
            r"Total Electric Charges  \$ [\d,.]+"
            r"\n.+)"  # Match an extra line ( this line sometimes contain bill usage data (see ladwp-multi.txt) )
        ),
        "sub_billing_period": (
            r"BILLING PERIOD  (?:\d+\/\d+\/\d+) - (?:\d+\/\d+\/\d+)[\s\S]+?"
            r"State Energy Surcharge - \d+ days\n(.+?kWh)[\s\S]+?"
            r"Electric Charges (\d+/\d+/\d+) - (\d+/\d+/\d+) \(\d+ Days\)[\s\S]+?"
            r"\$([\d,\.]+)[\s\S]+?"
            r"\$([\d,\.]+)"
        ),
        "sub_bill_data": (
            r"State Energy Surcharge - \d+ days\n(?P<used>.+?)kWh[\s\S]+?"
            r"Electric Charges (?P<start_date>\d+/\d+/\d+) - (?P<end_date>\d+/\d+/\d+) \(\d+ Days\)[\s\S]+?"
            r"\$(?:[\d,\.]+)[\s\S]+?"
            r"\$(?P<cost>[\d,\.]+)"
        ),
        "peaks": (
            r"(?:High Peak|Low Peak|Base) High Season Demand - .+days"
            r"[\s\S]+?(?P<peak>[\d,\.]+) kW x \$[\d\.]+\/kW"
        ),
        # Demand kW  Energy kWh
        "usage_type_1": r"([\d\.]+) kW  ([\d\.]+) kWh",
        #  High Peak kW  , Low Peak kW  , Base kW  , High Peak kWh  , Low Peak kWh  , Base kWh
        "usage_type_2": (
            r"(?P<high_peak_kw>[\d\.]+) kW  (?P<low_peak_kw>[\d\.]+) kW  (?P<base_kw>[\d\.]+) kW  "
            r"(?P<high_peak_kwh>[\d\.]+) kWh  (?P<low_peak_kwh>[\d\.]+) kWh  (?P<base_kwh>[\d\.]+) kWh"
        ),
        "bill_data": (
            r"State Energy Surcharge[\s\S]+?"
            r"(?P<used>.+?)kWh x \$([\d\.,]+)\/kWh[\s\S]+?"
            r"Total Electric Charges  \$ (?P<cost>[\d,\.]+)"
        ),
        "cost": r"Total Electric Charges  \$ ([\d,.]+)",
    }

    # "Bill Date:" is the same on every page of the pdf
    bill_date_str = re.search(regexes["bill_date"], pdf_text).group(1)
    bill_date = parse_date(bill_date_str).date()

    if commodity == "ccf":
        # Water/Fire bills
        bills = parse_ccf_bill(meter_number, pdf_text)
    else:
        bill_data_section_match = re.search(regexes["billing_section"], pdf_text)

        if not bill_data_section_match:
            raise Exception("Error parsing pdf: no billing section found")
        else:
            bill_data_section: str = bill_data_section_match.group(1)

        billing_periods = re.findall(regexes["billing_period"], bill_data_section)
        if not billing_periods:
            raise Exception("Error parsing pdf: no Billing Periods found")
        log.debug("billing_periods=%s", billing_periods)
        # Check if we have multiple billing periods in a bill
        if len(billing_periods) > 1:
            # We're only interested in the "sub" billing periods; delete the first billing
            # period line from the text, so that regexes["sub_billing_period"] doesn't match it.
            bill_data_section = re.sub(
                regexes["billing_period"], "", bill_data_section, count=1,
            )

            bill_data_subsections = re.finditer(
                regexes["sub_billing_period"], bill_data_section
            )

            for bill_data_subsection_match in bill_data_subsections:
                # Make a BillingDatum for each billing period
                bill_data_subsection = bill_data_subsection_match.group()
                bill_data_match = re.search(
                    regexes["sub_bill_data"], bill_data_subsection
                )
                peaks_match = re.findall(regexes["peaks"], bill_data_subsection)
                log.debug("bill_data=%s peaks=%s", bill_data_match, peaks_match)
                if bill_data_match:
                    datum = BillingDatum(
                        start=parse_date(bill_data_match.group("start_date")).date(),
                        end=parse_date(bill_data_match.group("end_date")).date()
                        - timedelta(days=1),
                        statement=bill_date,
                        cost=float(bill_data_match.group("cost").replace(",", "")),
                        used=float(bill_data_match.group("used").replace(",", "")),
                        peak=max([float(x.replace(",", "")) for x in peaks_match])
                        if peaks_match
                        else None,
                        attachments=None,
                        utility_code=None,
                        items=None,
                    )
                    log.info("multiple billing periods: data=%s", datum)
                    bills.append(datum)
        else:
            # Parse a regular bill with only one billing period
            billing_period_match = billing_periods[0]
            cost = used = peak = None
            if re.search(regexes["cost"], bill_data_section):
                cost = float(
                    re.search(regexes["cost"], bill_data_section)
                    .group(1)
                    .replace(",", "")
                )
            else:
                raise Exception("Error parsing pdf: couldn't extract cost")

            # There are multiple different ways the bill data is represented in the pdf...
            if re.search(regexes["usage_type_2"], bill_data_section):
                bill_data_match = re.search(regexes["bill_data"], bill_data_section)
                used = float(bill_data_match.group("used").replace(",", ""))
                cost = float(bill_data_match.group("cost").replace(",", ""))
                bill_data = re.search(regexes["usage_type_2"], bill_data_section)
                high_peak_kw = float(bill_data.group("high_peak_kw"))
                low_peak_kw = float(bill_data.group("low_peak_kw"))
                peak = max(high_peak_kw, low_peak_kw)
                log.debug(
                    "usage_type_2: bill_data_match=%s bill_data=%s",
                    bill_data_match,
                    bill_data,
                )
            elif re.search(regexes["usage_type_1"], bill_data_section):
                peak_str, used_str = re.search(
                    regexes["usage_type_1"], bill_data_section
                ).group(1, 2)
                peak = float(peak_str)
                used = float(used_str)
                log.debug("usage_type_1: peak=%s used=%s", peak, used)
            else:
                bill_data_match = re.search(regexes["bill_data"], bill_data_section)
                used = float(bill_data_match.group("used").replace(",", ""))
                cost = float(bill_data_match.group("cost").replace(",", ""))
                peak_matches = re.findall(regexes["peaks"], bill_data_section)
                peak = max([float(x.replace(",", "")) for x in peak_matches])
                log.debug("other: bill_data_match=%s", bill_data_match)

            if cost is not None and used is not None:
                datum = BillingDatum(
                    start=parse_date(billing_period_match[0]).date(),
                    end=parse_date(billing_period_match[1]).date() - timedelta(days=1),
                    statement=bill_date,
                    cost=cost,
                    used=used,
                    peak=peak,
                    attachments=None,
                    utility_code=None,
                    items=None,
                )
                log.info("single billing period: data=%s", datum)
                bills.append(datum)

    if "Corrections" in pdf_text:
        notify_rebill(meter_number, bill_date)

    return sorted(bills, key=lambda b: b.start)


class LADWPBillPdfConfiguration(Configuration):
    def __init__(self, meter_number: str, utility_account_id: str, commodity: str):
        super().__init__(scrape_bills=True)
        self.meter_number = meter_number
        self.utility_account_id = utility_account_id
        self.commodity = commodity


class LoginPage(CSSSelectorBasePageObject):
    """The ladwp.com home page, with username and password fields."""

    UsernameFieldSelector = r"#LoginForm\:pt_sf1\:username\:\:content"
    PasswordFieldSelector = r"#LoginForm\:pt_sf1\:password\:\:content"
    LoginButtonSelector = r"#LoginForm\:pt_sf1\:lgnbtn"
    SplashScreenSelector = ".af_document_splash-screen-cell"

    def wait_until_ready(self):
        log.info("Waiting for Login page to be ready")
        self._driver.wait().until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, self.UsernameFieldSelector)
            )
        )
        self._driver.wait().until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, self.PasswordFieldSelector)
            )
        )
        self._driver.wait().until(
            EC.presence_of_element_located((By.CSS_SELECTOR, self.LoginButtonSelector))
        )
        self._driver.wait().until(
            EC.invisibility_of_element_located(
                (By.CSS_SELECTOR, self.SplashScreenSelector)
            )
        )

    def login(self, username: str, password: str):
        """Authenticate with the web page.

        Fill in the username, password, then click "Log In"
        """
        log.info("Inserting credentials on login page.")
        self._driver.fill(self.UsernameFieldSelector, username)
        self._driver.fill(self.PasswordFieldSelector, password)
        self.find_element(self.LoginButtonSelector).click()


class MyAccountPage(CSSSelectorBasePageObject):
    """My Account page contains a captcha, but we only care about the left navbar buttons."""

    BillHistorySelector = 'a[title="Bill & Notification History"]'

    def wait_until_ready(self):
        log.debug("Waiting for Login page to be ready")
        self._driver.wait().until(
            EC.presence_of_element_located((By.CSS_SELECTOR, self.BillHistorySelector))
        )
        self._driver.sleep(5)

    def navigate_to_bill_history(self):
        log.info("clicking Bill & Notification History")
        self.find_element(self.BillHistorySelector).click()


class BillHistoryPage(CSSSelectorBasePageObject):

    BillHistoryHeaderXpath = (
        "//span[contains(.,'Bill & Notification History') and @class='hdng2']/.."
    )
    ReCaptchaIframeParentXpath = "//div[@class='g-recaptcha']//iframe[@height]/.."
    BillPdfsTableSelector = "table.paymentHistoryMidTitle.af_panelGroupLayout"

    def wait_until_ready(self):
        log.info("Waiting for Bill History Page to be ready")
        self._driver.wait().until(
            EC.presence_of_element_located((By.XPATH, self.BillHistoryHeaderXpath))
        )

        log.info(
            "Waiting for ReCaptcha to Appear"
        )  # should we add a special case for when a captcha isn't present?
        self._driver.wait().until(
            EC.presence_of_element_located((By.XPATH, self.ReCaptchaIframeParentXpath))
        )

    def wait_until_bills_ready(self):
        log.info("Waiting for Bills Pdf Table to be ready")
        self._driver.wait().until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, self.BillPdfsTableSelector)
            )
        )

    def select_account(self, account_id: str):
        log.info("selecting account %s" % account_id)
        select = Select(
            self._driver.find_element_by_css_selector(".rightPanelMyAcct select")
        )
        select.select_by_visible_text(account_id)
        log.debug("waiting for loading spinner to appear")
        self._driver.sleep(5)
        log.debug("waiting for loading spinner to disappear")
        self._driver.wait().until(
            EC.invisibility_of_element_located(
                (By.CSS_SELECTOR, ".AFBlockingGlassPane")
            )
        )

    def solve_captcha(self):
        iframe_parent = self._driver.find_element_by_xpath(
            self.ReCaptchaIframeParentXpath
        )
        # TODO: get page URL params from browser
        page_url = "https://www.ladwp.com/ladwp/faces/BillHistory?params_here"
        recaptcha_v2(self._driver, iframe_parent, page_url)

        self.find_element('a[title="Next"]').click()

    def download_bills(self, start: date, end: date):
        for link in self._driver.find_elements_by_css_selector(".af_commandImageLink"):
            bill_date_str = link.text.strip()

            try:
                bill_date = parse_date(bill_date_str).date()
            except Exception:
                # Probably not a date
                continue
            log.debug("found bill date %s", bill_date)
            if start <= bill_date <= end:
                log.info(f"Downloading Bill for date: {bill_date_str}")
                link.click()


class LADWPBillPdfScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "LADWP bill PDF"
        self.login_url = "https://ladwp.com/"
        self.bill_components = ["start", "end", "statement", "cost", "used", "peak"]

    @property
    def meter_number(self):
        return self._configuration.meter_number

    @property
    def commodity(self):
        return self._configuration.commodity

    def _execute(self):
        # Direct the driver to the login page
        self._driver.get(self.login_url)
        # Create page helpers
        login_page = LoginPage(self._driver)
        my_account_page = MyAccountPage(self._driver)
        bill_history_page = BillHistoryPage(self._driver)

        try:
            login_page.wait_until_ready()
        except Exception as exc:
            self.screenshot("login")
            raise exc
        login_page.login(self.username, self.password)
        self.screenshot("after login")

        my_account_page.wait_until_ready()
        my_account_page.navigate_to_bill_history()
        self.screenshot("bill history")

        bill_history_page.wait_until_ready()
        bill_history_page.solve_captcha()
        self.screenshot("after captcha")

        bill_history_page.wait_until_bills_ready()
        bill_history_page.select_account(self._configuration.utility_account_id)
        bill_history_page.wait_until_bills_ready()
        bill_history_page.download_bills(self.start_date, self.end_date)
        # get bills from download directory and parse

        bills: List[BillingDatum] = []
        prefix = f"{config.WORKING_DIRECTORY}/current"

        log.info("Waiting for downloads to finish")
        while any(".pdf.crdownload" in f for f in os.listdir(prefix)):
            # Wait for downloads to finish
            time.sleep(1)
            continue

        for filename in sorted(os.listdir(prefix)):
            if ".pdf" not in filename:
                continue

            log.info("parsing file %s" % filename)
            parsed_bills = parse_pdf(
                f"{prefix}/{filename}", self.meter_number, self.commodity
            )
            log.info(f"filename {filename} bills={parsed_bills}")
            if not parsed_bills:
                log.warning(f"no billing datum: filename={filename}")
                continue
            with open(prefix + "/" + filename, "rb") as pdf_data:
                bill = parsed_bills[0]
                key = hash_bill(
                    self._configuration.utility_account_id,
                    bill.start,
                    bill.end,
                    bill.cost,
                    bill.peak,
                    bill.used,
                )
                attachment_entry = upload_bill_to_s3(
                    BytesIO(pdf_data.read()),
                    key,
                    source="www.ladwp.com",
                    statement=bill.end,
                    utility="utility:ladwp",
                    utility_account_id=self._configuration.utility_account_id,
                )
            for bill in parsed_bills:
                bills.append(bill._replace(attachments=[attachment_entry]))

        return Results(bills=bills)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = LADWPBillPdfConfiguration(
        meter_number=meter.service_id,
        utility_account_id=meter.utility_service.utility_account_id,
        commodity=meter.commodity,
    )

    return run_datafeed(
        LADWPBillPdfScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
