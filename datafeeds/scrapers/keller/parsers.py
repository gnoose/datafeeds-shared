import io
from io import BytesIO
from datetime import datetime, date
import re
from typing import Optional, Tuple

import PyPDF2

from datafeeds.common.typing import BillingDatum
from datafeeds.common.util.pdfparser import pdf_bytes_to_str

BILL_AMOUNT = re.compile(r"TOTAL NEW CHARGES([\d,]*\.\d\d)")
SERVICE_PERIOD = re.compile(
    r"Service Period:\s*(\d\d/\d\d/\d{4})\s*To:\s*(\d\d/\d\d/\d{4})"
)

USE_BLOCK_A = re.compile(r"(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+Usage Month")
USE_BLOCK_B = re.compile(r"(\d+)\s+(\d+)\s+(\d+)\s+Usage Month")
USE_BLOCK_C = re.compile(r"(\d+)\s+(\d+)\s+Usage Month")
USE_BLOCK_D = re.compile(r"(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+Usage Month")
USE_BLOCK_E = re.compile(
    r"Service Period:\s*(\d\d/\d\d/\d{4})\s*To:\s*(\d\d/\d\d/\d{4})\s+Usage Month"
)
USE_BLOCK_F = re.compile(r"(\d+)\s+Total Usage")
# AMOUNT, two cost values, then two sets of readings
USE_BLOCK_G = re.compile(
    r"AMOUNT\s+\d+\.\d+\s+\d+\.\d+\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+Usage Month"
)
# BALANCE PRIOR TO LAST BILL, readings and diff, then ADJUSTMENTS
USE_BLOCK_H = re.compile(
    r"BALANCE PRIOR TO LAST BILL\s+(\d+)\s+(\d+)\s+(\d+)\s+ADJUSTMENTS"
)
# BALANCE PRIOR TO LAST BILL paired readings BALANCE FORWARD (DUE UPON RECEIPT)
USE_BLOCK_I = re.compile(
    r"BALANCE PRIOR TO LAST BILL\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+BALANCE FORWARD"
)
# PAYMENTS used ADJUSTMENTS
USE_BLOCK_J = re.compile(r"PAYMENTS\s+(\d+)\s+ADJUSTMENTS")


def determine_bill_amount(pdf_text: str) -> Optional[float]:
    match = BILL_AMOUNT.search(pdf_text)
    if match is None:
        return None

    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def determine_bill_period(pdf_text: str) -> Optional[Tuple[date, date]]:
    match = SERVICE_PERIOD.search(pdf_text)
    if match is None:
        return None

    try:
        start = datetime.strptime(match.group(1), "%m/%d/%Y").date()
        end = datetime.strptime(match.group(2), "%m/%d/%Y").date()
        return start, end
    except ValueError:
        return None


#
# Computing use is a little confusing. In the text we extract from the PDF, there will be between
# 2 and 5 numbers indicating meter readings and the reading difference.
#
# There are (apparently) three different cases:
# - A: 5 numbers: This looks like two submeters together. We need to verify
#   sum(submeter end readings) - sum(submeter start readings) = delta
# - B: 3 numbers: This looks like a single water meter. We need to verify end - start = delta.
# - C: 2 numbers: I think this only occurs if the meter was not used (e.g. a seasonal water
#      meter for a baseball field). In this case we verify start = end and report 0, or None
#      in any other case.
# - D: 4 numbers: This case only appears to arise when the customer has two submeters and no use.
#      Apparently when the start and end readings agree, the utility does not put the delta on the bill.
# - E: In some cases, if the meter shows no use, the utility does not print reading values at all.
# - F: This case is a catch-all. For bills with a more complicated metering structure, the total use
#      is always the third number in the first row of the use "table" in the bill. In this case,
#      we don't try to confirm that the use matches the difference between readings.
#
# - G: AMOUNT, two decimal numbers, then two sets of readings, then Usage Month
# - H: # BALANCE PRIOR TO LAST BILL, readings and diff, then ADJUSTMENTS
# - I: BALANCE PRIOR TO LAST BILL paired readings BALANCE FORWARD (DUE UPON RECEIPT)
# - J: PAYMENTS used ADJUSTMENTS
def determine_use_a(pdf_text: str) -> Optional[float]:
    match = USE_BLOCK_A.search(pdf_text)
    if match is None:
        return None

    try:
        read_start = int(match.group(1)) + int(match.group(2))
        read_end = int(match.group(3)) + int(match.group(4))
        delta = int(match.group(5))
    except ValueError:
        return None

    if delta == read_end - read_start:
        return float(delta)

    return None


def determine_use_b(pdf_text: str) -> Optional[float]:
    match = USE_BLOCK_B.search(pdf_text)
    if match is None:
        return None

    try:
        read_start = int(match.group(1))
        read_end = int(match.group(2))
        delta = int(match.group(3))
    except ValueError:
        return None

    if delta == read_end - read_start:
        return float(delta)

    return None


def determine_use_c(pdf_text: str) -> Optional[float]:
    match = USE_BLOCK_C.search(pdf_text)
    if match is None:
        return None

    try:
        read_start = int(match.group(1))
        read_end = int(match.group(2))
    except ValueError:
        return None

    if read_end == read_start:
        return 0.0

    return None


def determine_use_d(pdf_text: str) -> Optional[float]:
    match = USE_BLOCK_D.search(pdf_text)
    if match is None:
        return None

    try:
        read_start_1 = int(match.group(1))
        read_start_2 = int(match.group(2))
        read_end_1 = int(match.group(3))
        read_end_2 = int(match.group(4))
    except ValueError:
        return None

    if read_end_1 == read_start_1 and read_start_2 == read_end_2:
        return 0.0

    return None


def determine_use_e(pdf_text: str) -> Optional[float]:
    match = USE_BLOCK_E.search(pdf_text)
    if match is None:
        return None

    return 0.0


def determine_use_f(pdf_text: str) -> Optional[float]:
    match = USE_BLOCK_F.search(pdf_text)
    if match is None:
        return None

    try:
        return int(match.group(1))
    except ValueError:
        return None


def determine_use_g(pdf_text: str) -> Optional[float]:
    match = USE_BLOCK_G.search(pdf_text)
    if match is None:
        return None

    try:
        read_start = int(match.group(1)) + int(match.group(2))
        read_end = int(match.group(3)) + int(match.group(4))
    except ValueError:
        return None

    return float(read_end - read_start)


def determine_use_h(pdf_text: str) -> Optional[float]:
    match = USE_BLOCK_H.search(pdf_text)
    if match is None:
        return None

    try:
        read_start = int(match.group(1))
        read_end = int(match.group(2))
        delta = int(match.group(3))
    except ValueError:
        return None

    if delta == read_end - read_start:
        return float(delta)

    return None


def determine_use_i(pdf_text: str) -> Optional[float]:
    match = USE_BLOCK_I.search(pdf_text)
    if match is None:
        return None

    try:
        read_start = int(match.group(1)) + int(match.group(2))
        read_end = int(match.group(3)) + int(match.group(4))
    except ValueError:
        return None

    return float(read_end - read_start)


def determine_use_j(pdf_text: str) -> Optional[float]:
    match = USE_BLOCK_J.search(pdf_text)
    if match is None:
        return None

    try:
        return int(match.group(1))
    except ValueError:
        return None


def determine_use(pdf_text: str):
    cases = [
        determine_use_a,
        determine_use_b,
        determine_use_c,
        determine_use_d,
        determine_use_e,
        determine_use_f,
        determine_use_g,
        determine_use_h,
        determine_use_i,
        determine_use_j,
    ]
    for case in cases:
        result = case(pdf_text)
        if result is not None:
            return result / 748.052  # Convert from gallons to CCF

    return None


def parse_bill_pdf(pdf: BytesIO) -> Optional[BillingDatum]:
    try:
        data = pdf.read()

        reader = PyPDF2.PdfFileReader(io.BytesIO(data))
        extraction1 = ""
        p = None
        for n in range(0, reader.numPages):
            p = reader.getPage(n)
        if p is None:
            return None
        extraction1 += p.extractText()
        extraction2 = pdf_bytes_to_str(data)
    except Exception:
        return None

    with open("/tmp/keller1.txt", "w") as f:
        f.write(extraction1)
    with open("/tmp/keller2.txt", "w") as f:
        f.write(extraction2)
    amount = determine_bill_amount(extraction1)
    period = determine_bill_period(extraction1)
    use = determine_use(extraction2)
    if use is None:
        use = 0.0

    if amount is not None and period is not None and use is not None:
        return BillingDatum(
            start=period[0],
            end=period[1],
            statement=period[1],  # bill doesn't have a statement date; use end date
            cost=amount,
            used=use,
            peak=None,
            items=None,
            attachments=None,
            utility_code=None,
        )

    return None
