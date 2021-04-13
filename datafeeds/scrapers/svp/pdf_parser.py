import logging
import re

from io import BytesIO
from typing import List
from datetime import timedelta, date
from dateutil.parser import parse as parse_date
from dateutil.relativedelta import relativedelta

from datafeeds.common.typing import BillingDatum
from datafeeds.common.typing import BillingDatumItemsEntry
from datafeeds.common.upload import hash_bill, upload_bill_to_s3
from datafeeds.parsers import pdfparser

log = logging.getLogger(__name__)


def extract_cost(text):
    cost = float(
        re.search(r"Silicon Valley Power([$\d,.]+)", text)
        .group(1)
        .replace(",", "")
        .replace("$", "")
    )
    return cost


def extract_used(text):
    used = float(
        re.search(r"Energy([$\d,.]+)\s+([$\d,.]+) kWh", text)
        .group(2)
        .replace(",", "")
        .replace("$", "")
    )
    return used


def extract_demand(text):
    demand = float(
        re.search(r"DemandPower Factor:\s+([\d.%]+)\s+([$\d,.]+)\s+", text)
        .group(2)
        .replace(",", "")
    )
    return demand


def extract_dates(text):
    date_text = re.search(r"CurrentEWW((\d\d/\d\d)+)", text).group(1)
    # split E,W,W dates into ['mm/dd','mm/dd','mm/dd','mm/dd','mm/dd','mm/dd']...
    # the first three elements are start dates (E,W,W) and the last three elements are end_dates
    # it doesn't matter which one we pick for start/end because the dates are same for E,W and W ?
    eww_dates = re.findall(r"\d\d/\d\d", date_text)

    start_date = parse_date(eww_dates[0]).date()
    end_date = parse_date(eww_dates[-1]).date()

    # dates don't have years; adjust year if needed
    if start_date > end_date:
        start_date = start_date - relativedelta(years=1)
    return start_date, end_date


def extract_line_items(text) -> List[BillingDatumItemsEntry]:
    try:
        # from Meter Charge through Water / Sewer
        line_items_text = re.search("(Meter Charge.*?)Water", text).group(1)
    except AttributeError:
        return []

    line_items_regex = (
        r"Meter Charge(?P<charge_total>[-\$\d,\.]+)"
        r"Energy(?P<energy_total>[-\$\d,\.]+)  (?P<energy_quantity>[\d,\.]+) kWh X (?P<energy_rate>[-\$\d,\.]+)\/kWh = .+"
        r"Demand(?P<demand_total>[-\$\d,\.]+)"
        r"(Power Factor Charge(?P<pfc_total>[-\$\d,\.]+))?"
        r"Primary Voltage Discount(?P<pvd_total>[-\$\d,\.]+)"
        r"Public Benefit Charge(?P<pbc_total>[-\$\d,\.]+).*?"
        r"State Surcharge(?P<ss_total>[-\$\d,\.]+)"
    )
    match = re.search(line_items_regex, line_items_text)

    charge_total = float(match.group("charge_total").replace(",", "").replace("$", ""))

    energy_total, energy_quantity, energy_rate = match.group(
        "energy_total", "energy_quantity", "energy_rate"
    )

    energy_total = float(energy_total.replace(",", "").replace("$", ""))
    energy_quantity = float(energy_quantity.replace(",", "").replace("$", ""))
    energy_rate = float(energy_rate.replace(",", "").replace("$", ""))

    demand_total = float(match.group("demand_total").replace(",", "").replace("$", ""))

    # Bills may lack the Power Factor Charge
    if match.groupdict()["pfc_total"]:
        pfc_total = float(match.group("pfc_total").replace(",", "").replace("$", ""))
    else:
        pfc_total = None
    pvd_total = float(match.group("pvd_total").replace(",", "").replace("$", ""))
    pbc_total = float(match.group("pbc_total").replace(",", "").replace("$", ""))
    ss_total = float(match.group("ss_total").replace(",", "").replace("$", ""))

    meter = BillingDatumItemsEntry(
        description="Meter Charge",
        quantity=None,
        rate=None,
        total=charge_total,
        kind="other",
        unit=None,
    )

    energy = BillingDatumItemsEntry(
        description="Energy",
        quantity=energy_quantity,
        rate=energy_rate,
        total=energy_total,
        kind="use",
        unit="kWh",
    )

    demand = BillingDatumItemsEntry(
        description="Demand",
        quantity=None,
        rate=None,
        total=demand_total,
        kind="demand",
        unit="kW",
    )

    pfc = BillingDatumItemsEntry(
        description="Power Factor Charge",
        quantity=None,
        rate=None,
        total=pfc_total,
        kind="other",
        unit=None,
    )
    pvd = BillingDatumItemsEntry(
        description="Primary Voltage Discount",
        quantity=None,
        rate=None,
        total=pvd_total,
        kind="other",
        unit=None,
    )
    pbc = BillingDatumItemsEntry(
        description="Public Benefit Charge",
        quantity=None,
        rate=None,
        total=pbc_total,
        kind="other",
        unit=None,
    )
    state_surcharge = BillingDatumItemsEntry(
        description="State Surcharge",
        quantity=None,
        rate=None,
        total=ss_total,
        kind="other",
        unit=None,
    )

    return [meter, energy, demand, pfc, pvd, pbc, state_surcharge]


def process_pdf(
    utility: str,
    utility_account_id: str,
    service_id: str,
    statement_dt: date,
    pdf_filename: str,
) -> BillingDatum:
    log.info("Parsing text from PDF %s", pdf_filename)
    text = pdfparser.pdf_to_str(pdf_filename)

    cost = extract_cost(text)
    used = extract_used(text)
    demand = extract_demand(text)
    start_date, end_date = extract_dates(text)

    # if the start date is in the wrong year, replace year (start_date = 12/1, statement_dt=12/15/2020)
    if start_date > statement_dt:
        start_date = start_date.replace(year=statement_dt.year)
        end_date = end_date.replace(year=statement_dt.year)
    # end_date must be after start date (end_date = 1/5, start_date = 12/1)
    if end_date < start_date:
        end_date = end_date.replace(year=end_date.year + 1)

    # adjust end date because SVP bills overlap on start/end dates
    end_date = end_date - timedelta(days=1)
    line_items: List[BillingDatumItemsEntry] = extract_line_items(text)
    key = hash_bill(
        service_id,
        start_date,
        end_date,
        cost,
        demand,
        used,
    )
    with open(pdf_filename, "rb") as pdf_data:
        attachment_entry = upload_bill_to_s3(
            BytesIO(pdf_data.read()),
            key,
            source="mua.santaclaraca.gov",
            statement=end_date,
            utility=utility,
            utility_account_id=utility_account_id,
        )

    return BillingDatum(
        start=start_date,
        end=end_date,
        statement=statement_dt,
        cost=cost,
        used=used,
        peak=demand,
        items=line_items,
        attachments=[attachment_entry],
        utility_code=None,
    )
