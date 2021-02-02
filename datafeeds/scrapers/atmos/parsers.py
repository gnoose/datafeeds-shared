from datetime import datetime
from typing import Optional, List
from io import BytesIO
import re

import pandas as pd
import PyPDF2

from datafeeds.common.typing import BillingDatum


class AtmosParseError(Exception):
    pass


def parse_float(value) -> Optional[float]:
    try:
        return float(value)
    except ValueError:
        return None


def bill_data_from_xls(xls: bytes, service_account: str) -> List[BillingDatum]:
    df = pd.read_excel(BytesIO(xls))

    expected_columns = {
        "Service Account",
        "Current Charges",
        "From Billing Date",
        "To Billing Date",
        "Billed CCF",
    }
    actual_columns = set(df.columns)
    if not expected_columns < actual_columns:
        missing = ", ".join(list(expected_columns - actual_columns))
        raise AtmosParseError("Missing columns %s from spreadsheet." % missing)

    results = []

    for _, row in df.iterrows():
        if str(row["Service Account"]) == service_account:
            cost = parse_float(row["Current Charges"])
            used = parse_float(row["Billed CCF"])

            if cost is None or used is None:
                continue

            used = used * 1.036  # Convert CCF to therms.
            end = datetime.strptime(str(row["To Billing Date"]), "%Y%m%d").date()

            results.append(
                BillingDatum(
                    start=datetime.strptime(
                        str(row["From Billing Date"]), "%Y%m%d"
                    ).date(),
                    end=end,
                    statement=end,  # no separate statement date available
                    cost=cost,
                    used=used,
                    peak=None,
                    items=None,
                    attachments=None,
                    utility_code=None,
                )
            )

    return results


_service_account_pattern = re.compile(r"Account Number: (\d{10})")
_total_due_pattern = re.compile(r"Total Amount Due\$(\d+\.\d+)")
_use_pattern = re.compile(r"Usage in CCF:(\d+\.\d+)")
_empty_page_pattern = re.compile(r"\s*Page \d+ of \d+\s*")


def process_bill(
    text: str, service_account: str, meter_serial: str
) -> Optional[BillingDatum]:
    matches = _service_account_pattern.search(text)

    if not matches or matches.group(1) != service_account:
        return None

    bill_dates_pattern = (
        r"FromToPreviousPresent%s(\d+/\d+/\d\d)(\d+/\d+/\d\d)" % meter_serial
    )

    try:
        total_due = float(_total_due_pattern.search(text).group(1))
        date_match = re.search(bill_dates_pattern, text)
        start = datetime.strptime(date_match.group(1), "%m/%d/%y").date()
        end = datetime.strptime(date_match.group(2), "%m/%d/%y").date()
        use = (
            float(_use_pattern.search(text).group(1)) * 1.036
        )  # Convert CCF to therms.
    except (ValueError, AttributeError):
        return None

    return BillingDatum(
        start=start,
        end=end,
        statement=end,  # statement date is not visible in the bill PDF text; use end date
        cost=total_due,
        used=use,
        peak=None,
        items=None,
        attachments=None,
        utility_code=None,
    )


def group_pages(pages: List[str]) -> List[str]:
    """Concatenate the text from pages belonging to the same bill."""
    results = []
    buffer = ""

    for p in pages:
        if _service_account_pattern.search(p) and buffer:
            results.append(buffer)
            buffer = p
        else:
            buffer += p

    # Ensure we capture the final contents of the buffer.
    if buffer:
        results.append(buffer)

    return results


def bill_data_from_pdf(
    pdf: bytes, service_account: str, meter_serial: str
) -> List[BillingDatum]:

    # An Atmos PDF bill is one large PDF rollup for all of the per-meter bills in the account. The PDF has
    # three main sections:
    # - The first page, which summarizes the total amount due.
    # - A table summarizing the charges per meter. This could be several pages.
    # - An individual bill for each meter. Each individual bill has two pages (front and back of the paper bill).
    #
    # For our purposes, the third section of the PDF contains all of the data that we need
    # regarding billing.

    reader = PyPDF2.PdfFileReader(BytesIO(pdf))
    pages = [reader.getPage(n) for n in range(0, reader.numPages)]
    texts = [p.extractText() for p in pages]

    last_empty_page = -1
    for ii, t in enumerate(texts):
        if _empty_page_pattern.match(t):
            last_empty_page = ii

    start = last_empty_page + 1
    bill_pages = texts[start:]
    bills = group_pages(bill_pages)

    results = []
    for b in bills:
        bd = process_bill(b, service_account, meter_serial)
        if bd:
            results.append(bd)

    return results
