import logging
import re
from datetime import timedelta, date
from io import BytesIO
from typing import List, Tuple, Dict, Optional

from dateutil.parser import parse as parse_date

from datafeeds.common.typing import BillingDatumItemsEntry, BillingDatum
from datafeeds.common.upload import hash_bill, upload_bill_to_s3
from datafeeds.parsers import pdfparser

log = logging.getLogger(__name__)


EXCLUDE_LINE_ITEMS = [
    "Previous amount",
    "Payment received",
    "Total amount due",
    "Prior Balance",
    "Amount Due",
    "Late Payment Charge",
    "Previous Months Adjustment",
    "See Next Page for Meter Reading and Billing Details",
]


def _date_with_year(date_str: str, statement_dt: date) -> date:
    """Add a year to a date with only month and year.

    Use statement date year unless date is in Dec and statement date is in Jan.
    """
    if statement_dt.month == 1 and "dec" in date_str.lower():
        year = statement_dt.year - 1
    else:
        year = statement_dt.year
    return parse_date("%s %s" % (date_str, year)).date()


def extract_dates(text: str) -> Tuple[date, date, date]:
    """Get dates from a string like JUN 10 to JUL 9, with year from the statement date."""
    dt_str = re.search(r"Bill date\s+(\w+ \d+, \d+)", text, re.DOTALL).group(1)
    statement_dt = parse_date(dt_str).date()
    match = re.search(r"For service\s+(\w+ \d+) - (\w+ \d+)", text, re.DOTALL)
    to_dt = _date_with_year(match.group(2), statement_dt)
    from_dt = _date_with_year(match.group(1), statement_dt)
    # if statement date is Jan and statement period is Nov x - Dec y,
    # make sure from_dt is in same year as to_dt
    if from_dt.month == 11 and to_dt.month == 12 and from_dt.year != to_dt.year:
        from_dt = date(to_dt.year, from_dt.month, from_dt.day)
    return from_dt, to_dt, statement_dt


def extract_labels_then_values(cost_data: str) -> List[BillingDatumItemsEntry]:
    """Get charges where labels are a newline-delimited list, followed by values.

    Example:
        line-item 1
        line-item 2
        100.00
        90.00
    """
    labels: List[str] = []
    values: Dict[str, float] = {}
    label_idx = 0
    seen_dollar = False

    for idx, row in enumerate(cost_data.strip().split("\n")):
        val = row.strip()
        # first value usually has a $; want to distinguish from an address number
        if re.match(r"\$[\d,\.\-\$]", val):
            seen_dollar = True
        # but sometimes it's just a number, immediately after Amount Due
        if re.match(r"[\d,\.\-\$]", val) and "Amount Due" in labels:
            seen_dollar = True
        if seen_dollar:
            if label_idx >= len(labels):
                break
            label = labels[label_idx]
            log.debug("%s\t%s\t%s\t%s", idx, label, label_idx, val)
            if not val:
                val = "0"
            values.setdefault(label, 0.0)  # sum duplicate labels
            try:
                values[label] += float(re.sub(r"[,\$]", "", val))
            except ValueError:
                log.debug("%s is not a number", val)
            label_idx += 1
            if "Total" in label:
                break
        else:
            log.debug("label\t%s", val)
            labels.append(val)
    items = []
    # drop the last value: total amount due
    del values[label]
    for key in values:
        if True in {ex in key for ex in EXCLUDE_LINE_ITEMS}:
            log.info("skipping charge for %s", key)
            continue
        log.debug("cost item: %s\t%s", key, values[key])
        kind = "other"
        if "Demand" in key:
            kind = "demand"
        if "Energy" in key:
            kind = "use"
        items.append(
            BillingDatumItemsEntry(
                description=key,
                quantity=None,
                rate=None,
                total=values[key],
                kind=kind,
                unit=None,
            )
        )
    return items


def extract_cost_items(text: str) -> List[BillingDatumItemsEntry]:
    cost_data = re.search(
        r"Billing summary\n(.*?)How does this month compare", text, re.DOTALL
    )
    return extract_labels_then_values(cost_data.group(1))


def extract_use_and_demand(text: str) -> Tuple[float, Optional[float]]:
    match = re.search(
        r"Present\s+-\s+Previous\s+x\s+Multiplier\s+=\s+Total Usage(.*?)\x0c",
        text,
        re.DOTALL,
    )
    if match:
        # get values from Present vs Previous block
        use_data = match.group(1).strip()
        demand = max(
            [
                float(re.search(r"([,\d]+)", match).group(1).replace(",", ""))
                for match in re.findall(r"([,\d]+ KW)\s", use_data, re.DOTALL)
            ]
        )
        use = sum(
            [
                float(re.search(r"([,\d]+)", match).group(1).replace(",", ""))
                for match in re.findall(r"([,\d]+ KWH)", use_data, re.DOTALL)
            ]
        )
    else:
        # get from How does this month compare section
        use_data = re.search(r"Energy Used.*?([,\d]+) kWh", text, re.DOTALL).group(1)
        use = float(use_data.replace(",", ""))
        demand = None
    return use, demand


def parse_new_pdf(text: str) -> BillingDatum:
    """Duke changed their billing PDF format in June 2020. This parses the new format."""
    cost_items = extract_cost_items(text)
    cost = sum([item.total for item in cost_items])
    use, demand = extract_use_and_demand(text)
    start_dt, end_dt, statement_dt = extract_dates(text)
    return BillingDatum(
        start=start_dt,
        # adjust end date because Duke bills overlap on start/end dates
        end=end_dt - timedelta(days=1),
        statement=statement_dt,
        cost=round(cost, 2),
        used=use,
        peak=demand,
        items=cost_items,
        attachments=[],
        utility_code=None,
    )


def old_extract_dates(text: str) -> Tuple[date, date, date]:
    """Get dates from a string like JUN 10 to JUL 9, with year from the statement date."""
    dt_str = re.search(r"Bill Date.*?(\d\d/\d\d/\d\d\d\d)", text, re.DOTALL).group(1)
    statement_dt = parse_date(dt_str).date()
    match = re.search(r"Service From:.*?(\w+ \d+) to (\w+ \d+)", text, re.DOTALL)
    from_dt = _date_with_year(match.group(1), statement_dt)
    to_dt = _date_with_year(match.group(2), statement_dt)
    # if statement date is Jan and statement period is Nov x - Dec y,
    # make sure from_dt is in same year as to_dt
    if from_dt.month == 11 and to_dt.month == 12 and from_dt.year != to_dt.year:
        from_dt = date(to_dt.year, from_dt.month, from_dt.day)
    return from_dt, to_dt, statement_dt


def old_extract_cost_items(text: str) -> List[BillingDatumItemsEntry]:
    values: Dict[str, float] = {}
    # sometimes the header is Total Electric Usage
    match = re.search(r"(Total Electric Usage.*?)Amount Due", text, re.DOTALL)
    if not match:
        # sometimes it's Electricity Usage
        match = re.search(r"(Electricity Usage.*?)Amount Due", text, re.DOTALL)
    cost_data = match.group(1)
    # Sometimes there is not header at all. Check to see if line items include tax (Sales tax);
    # if not, this is probably the use vs previous block.
    if "tax" in cost_data.lower():
        multiline_match = re.match(
            r"Electricity Usage.*?([,\d]+).*?([,\-\.\d]+)", cost_data, re.DOTALL
        )
        for row in re.findall(r"(.*? [,\d\.-]+)", cost_data, re.DOTALL):
            match = re.search(r"(.*?)\s+([,\d\.-]+)", row.strip(), re.DOTALL)
            if not match:
                continue
            try:
                label = match.group(1).strip()
                # check for Electricity Usage followed by usage value, then amount
                if label == "Electricity Usage" and multiline_match:
                    val = multiline_match.group(2)
                else:
                    val = match.group(2)
                values[label] = float(re.sub(r"[,\$]", "", val))
            except ValueError:
                log.info("error parsing %s into a number", match.group(2))
    else:
        match = re.search(r"DESCRIPTION(.*?)AMOUNT", text, re.DOTALL)
        if match:
            return extract_labels_then_values(match.group(1))
        match = re.search(r"AMOUNT(.*?)METER", text, re.DOTALL)
        # previous bill amount, payments, new charges, adjustments, amount due
        amounts: List[str] = [
            line for line in match.group(1).split() if re.match(r"\s*\$[\d,\-]+", line)
        ]
        return [
            BillingDatumItemsEntry(
                description="New Charges",
                quantity=None,
                rate=None,
                total=float(amounts[2].replace("$", "").replace(",", "")),
                kind="use",
                unit=None,
            )
        ]

    items = []
    for key in values:
        if True in {ex in key for ex in EXCLUDE_LINE_ITEMS}:
            log.info("skipping charge for %s", key)
            continue
        items.append(
            BillingDatumItemsEntry(
                description=key,
                quantity=None,
                rate=None,
                total=values[key],
                kind="use" if "Usage" in key else "other",
                unit=None,
            )
        )
    return items


def old_extract_use_and_demand(text: str) -> Tuple[float, float]:
    peak = None
    use = 0.0
    in_demand = False
    in_use = False
    seen_total = False
    for line in text.split("\n"):
        if "KW" in line and "Demand" in line:
            log.debug("starting demand: %s", line)
            in_demand = True
            continue
        if "KWH" in line and (
            "Energy" in line or "Electricity Usage" in line or "Total KWH" in line
        ):
            log.debug("starting use: %s", line)
            if "Total KWH":
                seen_total = True
            in_use = True
            continue
        if in_demand or in_use:
            match = re.match(r"[,\d+]+", line.strip())
            val = float(match.group(0).replace(",", "")) if match else None
            log.debug("use/demand: %s", val)
            if val is not None:
                if in_demand:
                    in_demand = False
                    peak = val if peak is None else max(val, peak)
                if in_use:
                    in_use = False
                    if seen_total and use > 0:
                        # don't double count
                        continue
                    use += val
    return use, peak


def parse_old_pdf(text: str) -> BillingDatum:
    """Parse the old format bill PDF."""
    cost_items = old_extract_cost_items(text)
    cost = sum([item.total for item in cost_items])
    use, demand = old_extract_use_and_demand(text)
    start_dt, end_dt, statement_dt = old_extract_dates(text)
    return BillingDatum(
        start=start_dt,
        # adjust end date because Duke bills overlap on start/end dates
        end=end_dt - timedelta(days=1),
        statement=statement_dt,
        cost=round(cost, 2),
        used=use,
        peak=demand,
        items=cost_items,
        attachments=[],
        utility_code=None,
    )


def parse_pdf(pdf_filename: str, utility: str, utility_account_id: str) -> BillingDatum:
    text = pdfparser.pdf_to_str(pdf_filename)
    if "Your Energy Bill" in text:
        log.info("parsing new-style PDF %s", pdf_filename)
        data = parse_new_pdf(text)
    else:
        log.info("parsing old-style PDF %s", pdf_filename)
        data = parse_old_pdf(text)
    key = hash_bill(
        utility_account_id, data.start, data.end, data.cost, data.peak, data.used
    )
    with open(pdf_filename, "rb") as pdf_data:
        attachment_entry = upload_bill_to_s3(
            BytesIO(pdf_data.read()),
            key,
            source="www.duke-energy.com",
            statement=data.end,
            utility=utility,
            utility_account_id=utility_account_id,
        )
    return data._replace(attachments=[attachment_entry])
