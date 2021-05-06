import re
import time
import os
import logging

from io import BytesIO
from typing import Optional, List, Set

from dateutil.parser import parse as parse_date
from datetime import date, timedelta
from pdfminer.high_level import extract_pages
from pdfminer.layout import LTTextContainer
from selenium.common.exceptions import NoSuchElementException

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
from datafeeds.parsers.pacific_power import extract_pdf_text

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


def kw_regexes(meter_number: str):
    return {
        "meter_number": r"METER NUMBER  (.+)  \d+",
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
        "peaks_2": r"\n([\d\.]+) kW +([\d\.]+) kW +([\d\.]+) kW\n",
        # Demand kW  Energy kWh
        "usage_type_1": r"([\d\.]+) kW  ([\d\.]+) kWh",
        # from box above line items; may have a variable number of kW / kWh fields
        # 39.47 kW  38.28 kW  24.59 kW  1556 kWh  2064 kWh  5247 kWh
        "usage_box_1": r"\n([\d\.]+ +kW .*? kWh)\n",
        # usage box with kVarh
        # 878.4 kW  0 kW  892.8 kW  0 kW  619.2 kW  0 kW  234720 kWh  270720 kWh  567360 kWh  145440 kVarh  165600 kVarh  348480 kVarh
        # 0 kW  0 kW  0 kW  0 kWh  0 kWh  96 kWh  0 kVarh  96 kVarh  96 kVarh
        "usage_box_2": r"\n([\d\.]+ +kW .*? kVarh)\n",
        # usage box at the very end of the file
        "usage_box_3": r"([\d\.]+ .*?kWh)$",
        "bill_data": (
            r"State Energy Surcharge[\s\S]+?"
            r"(?P<used>.+?)kWh x \$([\d\.,]+)\/kWh[\s\S]+?"
            r"Total Electric Charges  \$ (?P<cost>[\d,\.]+)"
        ),
        "cost": r"Total Electric Charges  \$ ([\d,.]+)",
        # if billing_section not found, try these
        "alt1_date_usage": r"Electric Charges\s+(\d+/\d+/\d+) - (\d+/\d+/\d+)\s+([\d\.,]+) kWh",
        # dates but no usage
        "alt2_date_usage": r"Electric Charges\s+(\d+/\d+/\d+) - (\d+/\d+/\d+)\s+",
        "alt1_cost": r"Total \w+ Charges  \$ ([\d,.]+)",
        "cost_subtotal": r"Subtotal Electric Charges\n.*?Total Electric Charges\s+\$\s+(?P<cost>[\d\.,]+)",
        # requires re.DOTALL
        "alt1_peak": r"Total kWh used.*?([\d\.,]+) kW\s+([\d\.,]+) kWh",
        "alt_3_multi": r"Electric Charges\s+(\d+/\d+/\d+) - (\d+/\d+/\d+)\s+\(\d+ Days\) \$([\d,]*\.\d\d)",
    }


def _alternate_section(
    filename: str, bill_date: date, meter_number: str, pdf_text: str
) -> List[BillingDatum]:
    regexes = kw_regexes(meter_number)
    # try multiple bills option first:
    with open(filename, "rb") as f:
        pdf_data = f.read()
    # Use PyPDF2 here to extract the individual bill costs beside their bill dates.
    alt_pdf_text = extract_pdf_text(BytesIO(pdf_data))
    sub_bills = re.findall(regexes["alt_3_multi"], alt_pdf_text)
    if sub_bills:
        billing_data = []
        for bill in sub_bills:
            datum = BillingDatum(
                start=parse_date(bill[0]).date(),
                end=parse_date(bill[1]).date() - timedelta(days=1),
                statement=bill_date,
                cost=str_to_float(bill[2]),
                used=None,
                peak=None,
                attachments=None,
                utility_code=None,
                items=None,
            )
            billing_data.append(datum)
            log.info("alternate regex 3: data=%s", datum)
        return billing_data
    else:
        date_usage = re.search(regexes["alt1_date_usage"], pdf_text)
        if date_usage:
            used = str_to_float(date_usage.group(3))
        else:
            date_usage = re.search(regexes["alt2_date_usage"], pdf_text)
            used = 0
        cost = str_to_float(re.search(regexes["alt1_cost"], pdf_text).group(1))
        peak_match = re.search(regexes["alt1_peak"], pdf_text, re.DOTALL)
        if date_usage and cost:
            datum = BillingDatum(
                start=parse_date(date_usage.group(1)).date(),
                end=parse_date(date_usage.group(2)).date() - timedelta(days=1),
                statement=bill_date,
                cost=cost,
                used=used,
                peak=str_to_float(peak_match.group(1)) if peak_match else None,
                attachments=None,
                utility_code=None,
                items=None,
            )
            log.info("alternate regex 1: data=%s", datum)
            return [datum]
        raise Exception(
            "Error parsing pdf %s for %s: no billing section found",
            filename,
            meter_number,
        )


def _multi_period(
    bill_date: date, meter_number: str, bill_data_section: str
) -> List[BillingDatum]:
    bills: List[BillingDatum] = []
    regexes = kw_regexes(meter_number)
    # We're only interested in the "sub" billing periods; delete the first billing
    # period line from the text, so that regexes["sub_billing_period"] doesn't match it.
    bill_data_section = re.sub(
        regexes["billing_period"],
        "",
        bill_data_section,
        count=1,
    )
    bill_data_subsections = re.finditer(
        regexes["sub_billing_period"], bill_data_section
    )

    for bill_data_subsection_match in bill_data_subsections:
        # Make a BillingDatum for each billing period
        bill_data_subsection = bill_data_subsection_match.group()
        bill_data_match = re.search(regexes["sub_bill_data"], bill_data_subsection)
        peaks_match = re.findall(regexes["peaks"], bill_data_subsection)
        log.debug("bill_data=%s peaks=%s", bill_data_match, peaks_match)
        if bill_data_match:
            datum = BillingDatum(
                start=parse_date(bill_data_match.group("start_date")).date(),
                end=parse_date(bill_data_match.group("end_date")).date()
                - timedelta(days=1),
                statement=bill_date,
                cost=str_to_float(bill_data_match.group("cost")),
                used=str_to_float(bill_data_match.group("used")),
                peak=max([str_to_float(x) for x in peaks_match])
                if peaks_match
                else None,
                attachments=None,
                utility_code=None,
                items=None,
            )
            log.info("multiple billing periods: data=%s", datum)
            bills.append(datum)
    return bills


def _single_period(
    bill_date: date,
    filename: str,
    meter_number: str,
    billing_period_match: str,
    bill_data_section: str,
    pdf_text: str,
) -> List[BillingDatum]:
    """Parse a regular bill with only one billing period."""
    regexes = kw_regexes(meter_number)
    cost = used = peak = None
    if re.search(regexes["cost"], bill_data_section):
        cost = str_to_float(re.search(regexes["cost"], bill_data_section).group(1))
    else:
        raise Exception(
            "Error parsing pdf %s for %s: couldn't extract cost",
            filename,
            meter_number,
        )
    # There are multiple different ways the bill data is represented in the pdf...
    for idx in [1, 2, 3]:
        usage_match = re.search(regexes["usage_box_%s" % idx], bill_data_section)
        if usage_match:
            break
    log.debug("usage_match=%s" % idx)
    if usage_match:
        bill_data_match = re.search(regexes["bill_data"], bill_data_section)
        if not bill_data_match:
            bill_data_match = re.search(
                regexes["cost_subtotal"], bill_data_section, re.DOTALL
            )
        if bill_data_match:
            # used = str_to_float(bill_data_match.group("used"))
            cost = str_to_float(bill_data_match.group("cost"))
        else:
            cost = str_to_float(re.search(regexes["alt1_cost"], pdf_text).group(1))
        # ['878.4 kW  ', '0 kW  ', '892.8 kW  ', '0 kW  ', '619.2 kW  ', '0 kW  ', '234720 kWh  ', ...
        used = 0.0
        peak = None
        # ['0 kW', '0 kW', '0 kW', '0 kWh', '0 kWh', '96 kWh']
        for val in re.findall(r"([\d\.]+ +kWh?)", usage_match.group(1)):
            float_val = str_to_float(val)
            if "kWh" in val:
                used += float_val
            else:
                peak = max(peak, float_val) if peak is not None else float_val
    elif re.search(regexes["usage_type_1"], bill_data_section):
        peak_str, used_str = re.search(
            regexes["usage_type_1"], bill_data_section
        ).group(1, 2)
        peak = str_to_float(peak_str)
        used = str_to_float(used_str)
        log.debug("usage_type_1: peak=%s used=%s", peak, used)
    else:
        bill_data_match = re.search(regexes["bill_data"], bill_data_section)
        used = str_to_float(bill_data_match.group("used"))
        cost = str_to_float(bill_data_match.group("cost"))
        peak_matches = re.findall(regexes["peaks"], bill_data_section)
        peak = None
        if peak_matches:
            peak = max([str_to_float(x) for x in peak_matches])
        else:
            for exp in ["peaks_2", "usage_box_1"]:
                match = re.search(regexes[exp], bill_data_section)
                if match:
                    peak = max([str_to_float(x) for x in match.groups()])
                    break
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
        return [datum]
    return []


def parse_kw_bill(
    filename: str, bill_date: date, meter_number: str, pdf_text: str
) -> List[BillingDatum]:
    regexes = kw_regexes(meter_number)
    bill_data_section_match = re.search(regexes["billing_section"], pdf_text)

    if not bill_data_section_match:
        return _alternate_section(filename, bill_date, meter_number, pdf_text)

    bill_data_section: str = bill_data_section_match.group(1)
    billing_periods = re.findall(regexes["billing_period"], bill_data_section)
    if not billing_periods:
        raise Exception(
            "Error parsing pdf%s for %s: no Billing Periods found",
            filename,
            meter_number,
        )
    log.debug("billing_periods=%s", billing_periods)
    # Check if we have multiple billing periods in a bill
    if len(billing_periods) > 1:
        return _multi_period(bill_date, meter_number, bill_data_section)
    return _single_period(
        bill_date,
        filename,
        meter_number,
        billing_periods[0],
        bill_data_section,
        pdf_text,
    )


def parse_ccf_bill(meter_number: str, pdf_text: str) -> List[BillingDatum]:
    """Method for parsing Water and Fire Bills"""
    bills: List[BillingDatum] = []
    regexes = {
        "bill_date": r"BILL DATE  (.+)",
        "meter_number": r"METER NUMBER  (.+)  \d+",
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
        "fire_service_data_1": (
            r"Fire Service Charges\n"
            rf"SA # : {meter_number}[\s\S]+?"
            r"BILLING PERIOD  (?P<start_date>[\d\/]+) - (?P<end_date>[\d\/]+)[\s\S]+?"
            r"^(?P<used>[\d\.]+) HCF$[\s\S]+?"
            r"Total Fire Service Charges  \$ (?P<cost>[\d\.]+)"
        ),
        "fire_service_data_2": (
            rf"SA # : {meter_number}.*?"
            r"BILLING PERIOD +(?P<start_date>[\d\/]+) - (?P<end_date>[\d\/]+)[\s\S].*?"
            r"\n(?P<used>[\d\.,]+) HCF.*?"
            r"Total Fire Service Charges .*?(?P<cost>[\d\.,]+)"
        ),
        # without HCF value
        "fire_service_data_3": (
            rf"SA # : {meter_number}.*?"
            r"BILLING PERIOD +(?P<start_date>[\d\/]+) - (?P<end_date>[\d\/]+)[\s\S].*?"
            r"\n(?P<used>[\d\.,]+)\n.*?"
            r"Total Fire Service Charges .*?(?P<cost>[\d\.,]+)"
        ),
        "single_line_water": (
            r"Water Charges +(?P<start_date>\d+\/\d+\/\d+) - (?P<end_date>\d+\/\d+\/\d+) +"
            r"(?P<used>[\d,\.]+) HCF\n\$(?P<cost>[\d,\.]+)"
        ),
        "multi_line_water": (
            rf"SA # : {meter_number}.*?"
            r"BILLING PERIOD +(?P<start_date>[\d\/]+) - (?P<end_date>[\d\/]+)[\s\S].*?"
            r"\n(.*?)"
            r"Total Water Charges .*?(?P<cost>[\d\.,]+)"
        ),
        "multi_line_water_use": r"([\d\.,]+) HCF x \$[\d\.,]+/HCF",
    }

    bill_date_str = re.search(regexes["bill_date"], pdf_text).group(1)
    bill_date = parse_date(bill_date_str).date()
    bill_data_section_match = re.search(regexes["water_billing_section"], pdf_text)

    if not bill_data_section_match:
        # check if we have a Fire Service bill and parse that
        for idx in [1, 2, 3]:
            fire_data_match = re.search(
                regexes[f"fire_service_data_{idx}"], pdf_text, re.MULTILINE | re.DOTALL
            )
            if fire_data_match:
                break
        if fire_data_match:
            bills.append(
                BillingDatum(
                    start=parse_date(fire_data_match.group("start_date")).date(),
                    end=parse_date(fire_data_match.group("end_date")).date()
                    - timedelta(days=1),
                    statement=bill_date,
                    cost=str_to_float(fire_data_match.group("cost")),
                    used=str_to_float(fire_data_match.group("used")),
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
        water_match = re.search(regexes["single_line_water"], pdf_text)
        if not water_match:
            water_match = re.search(regexes["multi_line_water"], pdf_text, re.DOTALL)

        # Check if we have multiple billing periods in a bill
        if len(billing_periods) > 1:
            # We're only interested in the "sub" billing periods, delete the first billing
            # period line from the text, so that regexes["billing_period"] doesn't match it.
            bill_data_section_text = re.sub(
                regexes["billing_period"],
                "",
                bill_data_section_text,
                count=1,
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
                            end=parse_date(dates_match.group(2)).date()
                            - timedelta(days=1),
                            statement=bill_date,
                            cost=round(
                                sum(
                                    [
                                        str_to_float(x[0]) * str_to_float(x[1])
                                        for x in cost_match
                                    ]
                                ),
                                2,
                            ),
                            used=round(
                                sum([str_to_float(x[0]) for x in cost_match]), 5
                            ),
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
        elif water_match:
            if "used" in water_match.groupdict():
                used = str_to_float(water_match.group("used"))
            else:
                used = sum(
                    [
                        str_to_float(u)
                        for u in re.findall(
                            regexes["multi_line_water_use"], water_match.group(3)
                        )
                    ]
                )
            bills.append(
                BillingDatum(
                    start=parse_date(water_match.group("start_date")).date(),
                    end=parse_date(water_match.group("end_date")).date()
                    - timedelta(days=1),
                    statement=bill_date,
                    cost=str_to_float(water_match.group("cost")),
                    used=used,
                    peak=None,
                    attachments=None,
                    utility_code=None,
                    items=None,
                )
            )

    # close up one day gaps; sometimes bill end dates don't need to be adjusted
    final_bills: List[BillingDatum] = []
    sorted_bills = sorted(bills, key=lambda b: b.start)
    for idx, bill in enumerate(sorted_bills):
        curr_bill = bill
        next_bill = sorted_bills[idx + 1] if idx + 1 < len(sorted_bills) else None
        if next_bill and (next_bill.start - bill.end).days == 2:
            curr_bill = bill._replace(end=bill.end + timedelta(days=1))
        final_bills.append(curr_bill)
    return final_bills


def str_to_float(val: str) -> float:
    """Convert a string to a float; remove characters other than digits . and ,"""
    return float(re.sub(r"[^\d\.-]", "", val))


def parse_pdf(filename: str, meter_number: str, commodity: str) -> List[BillingDatum]:
    """Parse a PDF and return a list of BillingDatum objects, sorted by start date."""

    pdf_text: str = get_pdf_text(filename)
    # "Bill Date:" is the same on every page of the pdf
    match = re.search(r"BILL DATE  (.+)", pdf_text)
    if not match:
        log.warning("Not a bill")
        return []
    bill_date = parse_date(match.group(1)).date()

    if "Corrections" in pdf_text:
        notify_rebill(meter_number, bill_date)

    if commodity == "ccf":
        # Water/Fire bills
        bills = parse_ccf_bill(meter_number, pdf_text)
    else:
        bills = parse_kw_bill(filename, bill_date, meter_number, pdf_text)
    return sorted(bills, key=lambda b: b.start)


class LADWPBillPdfConfiguration(Configuration):
    def __init__(
        self,
        meter_number: str,
        utility_account_id: str,
        commodity: str,
        account_name: str,
    ):
        super().__init__(scrape_bills=True)
        self.meter_number = meter_number
        self.utility_account_id = utility_account_id
        self.commodity = commodity
        self.account_name = account_name


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

    def too_many_sessions(self):
        self._driver.wait().until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".rightPanelMyAcct"))
        )
        text = self._driver.find_element_by_css_selector(".rightPanelMyAcct").text
        if "This web user has reached too many sessions" in text:
            return True
        return False

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

    def select_account(self, account_id: str, account_name: str):
        log.info("selecting account %s" % account_id)
        try:
            select = Select(
                self._driver.find_element_by_css_selector(".rightPanelMyAcct select")
            )
        except NoSuchElementException:
            log.info("no account select; single account")
            return
        try:
            select.select_by_visible_text(account_id)
        except NoSuchElementException as exc:
            # try account name if there is one
            if not account_name:
                raise exc
            log.debug("trying account name %s", account_name)
            select.select_by_visible_text(account_name)
        log.debug("waiting for loading spinner to appear")
        self._driver.sleep(5)
        log.debug("waiting for loading spinner to disappear")
        self._driver.wait().until(
            EC.invisibility_of_element_located(
                (By.CSS_SELECTOR, ".AFBlockingGlassPane")
            )
        )
        time.sleep(5)

    def solve_captcha(self) -> bool:
        iframe_parent = self._driver.find_element_by_xpath(
            self.ReCaptchaIframeParentXpath
        )
        page_url = self._driver.current_url
        if not recaptcha_v2(self._driver, iframe_parent, page_url):
            log.warning("failed captcha solving")
            return False

        self.find_element('a[title="Next"]').click()
        return True

    def download_bills(self, start: date, end: date):
        for link in self._driver.find_elements_by_css_selector(".af_commandImageLink"):
            bill_date_str = link.text.strip()
            log.debug("found bill link %s", bill_date_str)

            try:
                bill_date = parse_date(bill_date_str).date()
            except Exception:
                # Probably not a date
                continue
            log.debug("found bill date %s", bill_date)
            if start <= bill_date <= end:
                log.info(f"Downloading Bill for date: {bill_date_str}")
                link.click()

    def logout(self):
        # try to avoid This web user has reached too many sessions
        self._driver.find_element_by_xpath("//a[contains(text(), 'Log out')]").click()


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
        except Exception:
            self.screenshot("initial page load failed")
            # try one more time
            self._driver.get(self.login_url)
            login_page.wait_until_ready()
        login_page.login(self.username, self.password)
        self.screenshot("after login")

        my_account_page.wait_until_ready()
        my_account_page.navigate_to_bill_history()
        self.screenshot("bill history")

        if bill_history_page.too_many_sessions():
            # waiting 5 minutes doesn't seem to help
            bill_history_page.logout()
            raise Exception("too many sessions")
        bill_history_page.wait_until_ready()
        self.screenshot("after captcha")
        if not bill_history_page.solve_captcha():
            bill_history_page.logout()
            raise Exception("captcha failed")

        bill_history_page.wait_until_bills_ready()
        bill_history_page.select_account(
            self._configuration.utility_account_id, self._configuration.account_name
        )
        bill_history_page.wait_until_bills_ready()
        bill_history_page.download_bills(self.start_date, self.end_date)
        bill_history_page.logout()
        # get bills from download directory and parse

        bills: List[BillingDatum] = []
        prefix = f"{config.WORKING_DIRECTORY}/current"

        log.info("Waiting for downloads to finish")
        while any(".pdf.crdownload" in f for f in os.listdir(prefix)):
            # Wait for downloads to finish
            time.sleep(1)
            continue

        start_dates: Set[date] = set()
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
                attachments = [attachment_entry]
                if bill.start in start_dates:
                    # if we already have a bill with this start date, replace it
                    prev_bill = [b for b in bills if b.start == bill.start][0]
                    log.info(
                        "duplicate bill start: prev_bill = %s, bill = %s",
                        prev_bill,
                        bill,
                    )
                    bills.remove(prev_bill)
                    # copy the attachment
                    attachments += prev_bill.attachments
                bills.append(bill._replace(attachments=attachments))
                start_dates.add(bill.start)

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
        account_name=(datasource.meta or {}).get("accountName"),
    )

    # If meter has a recent bill, don't go to website since ladwp.com is fragile.
    # last_closing is last element of tuple
    latest_closing = meter.bills_range[-1]
    if latest_closing and latest_closing >= date.today() - timedelta(days=21):
        log.info("latest bill is fresh (%s); stopping now", latest_closing)
        return Status.COMPLETED
    return run_datafeed(
        LADWPBillPdfScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
