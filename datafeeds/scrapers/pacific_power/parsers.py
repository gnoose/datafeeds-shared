import io
from io import BytesIO
from datetime import date
import re
from typing import Optional, Tuple

from dateutil.parser import parse as parse_dt
import PyPDF2

from datafeeds.common import BillingDatum
from datafeeds.common.util.pdfparser import pdf_pages

"""
Demand and use values are extracted from tables that list these values alongside the relevant meter number.
When the PDF is parsed, these rows look like the following rows:

66887643Jun 3, 2019Jul 2, 20192931499384711.06,972 kwh
66887643DemandJul 2, 201926.381.026 kw

There may be multiple demand/use rows to capture TOU rates.

The parser makes the following assumptions:
- The meter multiplier (which we don't care about) is listed with a single decimal place.
  We need this assumption in order to separate the demand/use value from the meter multiplier.
- Bill details (there can be multiple meters per bill PDF) are separated with headers of the form
    "ITEM %d - <Some text>".
"""


AMOUNT_REGEX = re.compile(r"Total New Charges([\d,]+\.\d\d)")
USE_REGEX = re.compile(r"")
DEMAND_REGEX = re.compile(r"")


def extract_amount(text: str) -> Optional[float]:
    matches = AMOUNT_REGEX.search(text)
    if not matches:
        return 0

    try:
        return float(matches.group(1).replace(",", ""))
    except ValueError:
        return None


def extract_period(text: str, meter_number: str) -> Optional[Tuple[date, date]]:
    period_regex = (
        r"%s([A-Z][a-z]{2} \d+, \d{4})([A-Z][a-z]{2} \d+, \d{4})" % meter_number
    )
    matches = re.search(period_regex, text)
    if not matches:
        return None

    try:
        start = parse_dt(matches.group(1)).date()
        end = parse_dt(matches.group(2)).date()
        return start, end
    except ValueError:
        return None


def extract_use(text: str, meter_number: str) -> Optional[float]:
    rows = [
        x.replace(" onkwh", "").replace(" offkwh", "").replace(" kwh", "")
        for x in re.split(meter_number, text)
        if "kwh" in x and not x.startswith("Demand")
    ]

    use_values = []
    for row in rows:
        try:
            matches = re.search(r"\.\d([\d,]+)$", row)
            if not matches:
                continue
            use_values.append(float(matches.group(1).replace(",", "")))
        except ValueError:
            continue

    if use_values:
        return sum(use_values)

    return None


def extract_peak(text: str, meter_number: str) -> Optional[float]:
    rows = [
        x.replace(" onkw", "").replace(" offkw", "").replace(" kw", "")
        for x in re.split(meter_number, text)
        if "Demand" in x
    ]

    if rows and "Next scheduled" in rows[-1]:
        # This is the last row in the table, need to truncate the remainder of the bill's text.
        rows[-1] = re.split("Next scheduled", rows[-1])[0]

    demand_values = []
    for row in rows:
        try:
            matches = re.search(r"\.\d([\d,]+)$", row)
            if not matches:
                continue
            demand_values.append(float(matches.group(1).replace(",", "")))
        except ValueError:
            continue

    if demand_values:
        return max(demand_values)

    return None


def parse_bill_text(text: str, meter_number: str) -> Optional[BillingDatum]:
    sections = [
        s
        for s in re.split(r"ITEM \d+ - ", text)
        if s.startswith("ELECTRIC SERVICE") and meter_number in s
    ]

    if not sections:
        return None

    amount = extract_amount(sections[0])
    period = extract_period(sections[0], meter_number)
    use = extract_use(sections[0], meter_number)
    peak = extract_peak(sections[0], meter_number)

    if (
        amount is not None
        and period is not None
        and use is not None
        and peak is not None
    ):
        return BillingDatum(
            start=period[0],
            end=period[1],
            cost=amount,
            used=use,
            peak=peak,
            items=None,
            attachments=None,
        )

    return None


def extract_pdf_text(pdf: BytesIO):
    try:
        data = pdf.read()
        reader = PyPDF2.PdfFileReader(io.BytesIO(data))
        text = ""
        for n in range(0, reader.numPages):
            p = reader.getPage(n)
            text += p.extractText()
        return text
    except Exception:
        return ""


def parse_bill_pdf(pdf: BytesIO, meter_number: str) -> Optional[BillingDatum]:
    # Historical and recent PDFs are not encoded the same way, so we run two different
    # parsers to extract text from them.
    # Fortunately, the steps for parsing the text are the same.
    extract_1 = extract_pdf_text(pdf)
    bill = parse_bill_text(extract_1, meter_number)

    if bill:
        return bill

    extract_2 = "".join(pdf_pages(pdf))
    return parse_bill_text(extract_2, meter_number)
