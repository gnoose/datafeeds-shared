from enum import Enum

import logging
import csv
from datetime import timedelta, date, datetime

from deprecation import deprecated
import os
from sqlalchemy.orm.attributes import flag_modified
from typing import Optional, Union, BinaryIO, List, Dict, Set, Any, Tuple
from io import BytesIO
import hashlib
from sqlalchemy import func

from datafeeds import config, db
from datafeeds.common import index, platform
from datafeeds.common.partial_billing import PartialBillProcessor
from datafeeds.common.typing import (
    BillingData,
    show_bill_summary,
    BillingDatum,
    AttachmentEntry,
    BillPdf,
    Status,
)
from datafeeds.common import interval_transform
from datafeeds.common.util.s3 import (
    upload_pdf_to_s3,
    remove_file_from_s3,
    s3_key_exists,
)
from datafeeds.models import UtilityService
from datafeeds.models.bill import (
    Bill,
    PartialBillProviderType,
    snap_first_start,
    PartialBill,
)
from datafeeds.models.meter import Meter, MeterReading
from datafeeds.models.bill_document import BillDocument

BytesLikeObject = Union[BinaryIO, BytesIO]

log = logging.getLogger(__name__)

UPLOAD_DATA_BATCH_SIZE = 20


def _latest_closing(said) -> Optional[date]:
    query = db.session.query(
        func.max(Bill.closing).label("most_recent_closing")
    ).filter(Bill.service == UtilityService.oid, UtilityService.service_id == said)
    res = query.first()
    return res.most_recent_closing if res else None


def verify_bills(meter_oid: int, billing_data: BillingData) -> BillingData:
    """If we retrieved a bills with 0 cost and non-zero use, see if we can get cost from a current bill."""
    current_bills: Dict[date, Bill] = {}
    for row in db.session.query(Bill).filter(
        Bill.service == Meter.service, Meter.oid == meter_oid
    ):
        current_bills[row.closing] = row
    data: BillingData = []
    for bill in billing_data:
        if not (bill.cost == 0 and bill.used > 0):
            data.append(bill)
            continue
        log.warning("potential bad bill: cost is zero and use is not: %s", bill)
        current = current_bills.get(bill.end)
        if not current:
            data.append(bill)
            continue
        if current.cost > 0:
            log.info(
                "replacing 0 cost with existing cost %s from bill %s",
                current.cost,
                current.oid,
            )
            data.append(bill._replace(cost=current.cost))
        else:
            data.append(bill)
    return data


def upload_bills(
    meter_oid: int,
    service_id: str,
    task_id: str,
    scraper: str,
    billing_data: BillingData,
) -> Status:
    cur_most_recent = _latest_closing(service_id)

    _, change_records = _upload_bills_to_services(service_id, billing_data)

    if task_id and config.enabled("ES_INDEX_JOBS"):
        log.info("Updating billing range in Elasticsearch.")
        index.update_billing_range(task_id, billing_data)
        if change_records:
            index.index_bill_records(scraper, change_records)
    billing_data = verify_bills(meter_oid, billing_data)

    title = "Final Scraped Summary"
    show_bill_summary(billing_data, title)

    path = os.path.join(config.WORKING_DIRECTORY, "bills.csv")
    end = date(year=1900, month=1, day=1)
    with open(path, "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Service ID", "Start", "End", "Cost", "Used", "Peak"])
        for b in billing_data:
            writer.writerow([service_id, b.start, b.end, b.cost, b.used, b.peak])
            if type(b.end) == datetime:
                end = max(b.end.date(), end)  # type: ignore
            else:
                if b.end > end:
                    end = b.end
    log.info("Wrote bill data to %s." % path)
    if cur_most_recent and (end > cur_most_recent):
        return Status.SUCCEEDED
    return Status.COMPLETED


def upload_readings(
    transforms, meter_oid: int, scraper: str, task_id: str, readings
) -> Status:
    updated: List[MeterReading] = []
    if readings:
        readings = interval_transform.transform(
            transforms, task_id, scraper, meter_oid, readings
        )
        log.info("writing interval data to the database for %s %s", scraper, meter_oid)
        updated = MeterReading.merge_readings(
            MeterReading.from_json(meter_oid, readings)
        )

    if task_id and config.enabled("ES_INDEX_JOBS"):
        index.set_interval_fields(task_id, updated)

    log.info("Final Interval Summary")
    for when, intervals in readings.items():
        none_count = sum(1 for x in intervals if x is None)
        factor = (24 / len(intervals)) if len(intervals) > 0 else 1.0
        kWh = sum(x for x in intervals if x is not None) * factor
        log.info(
            "%s: %d intervals. %.1f net kWh, %d null values."
            % (when, len(intervals), kWh, none_count)
        )

    path = os.path.join(config.WORKING_DIRECTORY, "readings.csv")
    with open(path, "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Service", "Date", "Readings"])
        for when, intervals in readings.items():
            writer.writerow([meter_oid, str(when)] + [str(x) for x in intervals])
    log.info("Wrote interval data to %s." % path)

    if updated:
        return Status.SUCCEEDED
    return Status.COMPLETED


class AttachStatus(Enum):
    ATTACHED = "attached"
    FOUND = "found"
    NOT_ATTACHED = "not_attached"

    @classmethod
    def ordered(cls) -> List["AttachStatus"]:
        """Return status values in order from best (first) to worst (last)."""
        return [AttachStatus.ATTACHED, AttachStatus.FOUND, AttachStatus.NOT_ATTACHED]

    @classmethod
    def best(cls, statuses: List[Optional["AttachStatus"]]) -> Optional["AttachStatus"]:
        for status in AttachStatus.ordered():
            if status in statuses:
                return status
        return None


def add_attachment_to_bills(
    pdf: BillPdf, bills: List[Union[Bill, PartialBill]]
) -> AttachStatus:
    att_status = AttachStatus.NOT_ATTACHED
    for bill in bills:
        bill_type = (
            "bill" if isinstance(bill, Bill) else f"{bill.provider_type} partial bill"
        )
        # [{"kind": "bill", "key": "1ea5a6c9-0a3c-d0fc-a0ba-0eae4f86ddeb.pdf", "format": "PDF"}]
        log.info(
            "matched bill pdf statement %s, interval start %s to %s %s %s-%s",
            pdf.statement,
            pdf.start,
            bill_type,
            bill.oid,
            bill.initial,
            bill.closing,
        )
        current_pdfs = {
            att["key"] for att in bill.attachments or [] if att.get("format") == "PDF"
        }
        if current_pdfs:
            if pdf.s3_key in current_pdfs:
                att_status = AttachStatus.best([att_status, AttachStatus.FOUND])
                log.info(
                    "attachment %s already attached to %s %s",
                    pdf.s3_key,
                    bill_type,
                    bill.oid,
                )
                continue
        if not bill.attachments:
            bill.attachments = []
        # Insert new PDF's at the beginning so they are surfaced in the UI.
        bill.attachments.insert(0, {"kind": "bill", "key": pdf.s3_key, "format": "PDF"})
        flag_modified(bill, "attachments")
        db.session.add(bill)
        log.info("adding attachment %s to %s %s", pdf.s3_key, bill_type, bill.oid)
        att_status = AttachStatus.ATTACHED
    return att_status


def attach_bill_pdfs(
    meter_oid: int,
    task_id: str,
    meter_only: bool,
    pdfs: List[BillPdf],
) -> Status:
    """Attach a list of bill PDF files uploaded to S3 to bill records."""
    if not pdfs:
        return Status.COMPLETED

    count = 0
    unused = []
    for pdf in pdfs:
        log.info(
            "bill PDF for utility_account_id=%s statement=%s",
            pdf.utility_account_id,
            pdf.statement,
        )

        if meter_only:
            # Attach PDFs to bills on this meter's service only.  Matches up PDF start dates with PartialBill/
            # Bill start dates with a small buffer.
            bill_query = db.session.query(Bill).filter(
                Bill.service == Meter.service,
                Meter.oid == meter_oid,
                Bill.initial >= pdf.start - timedelta(days=1),
                Bill.initial <= pdf.start + timedelta(days=1),
            )
            partial_bill_query = db.session.query(PartialBill).filter(
                PartialBill.service == Meter.service,
                Meter.oid == meter_oid,
                PartialBill.initial >= pdf.start - timedelta(days=1),
                PartialBill.initial <= pdf.start + timedelta(days=1),
                PartialBill.superseded_by.is_(None),
                PartialBill.visible.is_(True),
                PartialBill.provider_type == PartialBillProviderType.TND_ONLY.value,
            )
            if not bill_query.count() and not partial_bill_query.count():
                log.warning(
                    "no bills found for utility_account_id %s %s-%s",
                    pdf.utility_account_id,
                    pdf.start,
                    pdf.end,
                )
            bill_attach_status = add_attachment_to_bills(pdf, bill_query)
            partial_attach_status = add_attachment_to_bills(pdf, partial_bill_query)
            if (
                AttachStatus.best([bill_attach_status, partial_attach_status])
                == AttachStatus.ATTACHED
            ):
                # Only increase count if attachments were updated.
                # Not adding any PDF's to "unused" because attachment could be in use on another meter.
                count += 1

        else:
            # Attach PDF's to potentially multiple bills on multiple services with the same utility account id.
            # Attach PDFs to bills on account that ended recently before the statement date:
            query = (
                db.session.query(Bill)
                .filter(UtilityService.utility_account_id == pdf.utility_account_id)
                .filter(UtilityService.oid == Bill.service)
                .filter(Bill.closing > pdf.statement - timedelta(days=14))
                .filter(Bill.closing <= pdf.statement)
            )
            bill_count = query.count()
            if not bill_count:
                log.warning(
                    "no bills found for utility_account_id %s %s-%s",
                    pdf.utility_account_id,
                    pdf.start,
                    pdf.end,
                )
            attached = add_attachment_to_bills(pdf, query)
            if attached == AttachStatus.ATTACHED:
                count += 1
            elif attached == AttachStatus.NOT_ATTACHED:
                unused.append(pdf.s3_key)
    log.info("attached %s/%s pdfs", count, len(pdfs))
    for key in unused:
        remove_file_from_s3(config.BILL_PDF_S3_BUCKET, key)
    if task_id and config.enabled("ES_INDEX_JOBS"):
        log.info("Updating billing range in Elasticsearch.")
        index.update_bill_pdf_range(task_id, meter_oid, pdfs)

    if count:
        return Status.SUCCEEDED
    return Status.COMPLETED


def upload_partial_bills(
    meter: Meter,
    task_id: str,
    billing_data: BillingData,
    bill_type: PartialBillProviderType,
) -> Status:
    """
    Goes through billing_data and uploads new partial bills directly to the partial bills table.
    If a new partial bill differs from an existing partial bill,
    a new bill is created, rather than overwriting the old one.

    New partial bills are written directly to the db; they do not go through platform.
    """
    log.info("Starting processing of partial bill scraper results.")
    processor = PartialBillProcessor(meter, bill_type, billing_data)
    status = processor.process_partial_bills()
    processor.log_summary()
    if task_id and config.enabled("ES_INDEX_JOBS"):
        log.info("Updating billing range in Elasticsearch.")
        index.update_billing_range(task_id, billing_data)

    return status


def _upload_bills_to_services(
    service_id: str, billing_data: BillingData
) -> Tuple[List[Bill], List[Dict[str, Any]]]:
    """Reconciles incoming billing data with bills on every service with matching service_id."""
    services = db.session.query(UtilityService.oid, Meter.oid).filter(
        UtilityService.service_id == service_id,
        Meter.service == UtilityService.oid,
    )

    updated: Set[Bill] = set()
    change_records: List[Dict[str, Any]] = []
    for service_oid, meter_oid in services:
        existing = (
            db.session.query(Bill)
            .filter(service_oid == Bill.service)
            .order_by(Bill.initial.asc())
            .all()
        )

        snapped_billing_data = snap_first_start(billing_data, existing)
        uncommitted_bills = convert_billing_data_to_bills(
            service_oid, snapped_billing_data
        )

        new_bills, records = Bill.add_bills(uncommitted_bills, source="datafeeds")
        # add meter to record; one bill can be added to multiple meters with the same service_id
        for record in records:
            record["meter"] = meter_oid
        change_records += records
        updated = updated.union(new_bills)
    return list(updated), change_records


def convert_billing_data_to_bills(
    service_oid: int, billing_data: BillingData
) -> List[Bill]:
    bills: List = []
    for billing_datum in billing_data:
        if not billing_datum:
            continue

        initial = billing_datum.start
        closing = billing_datum.end
        if isinstance(billing_datum.start, datetime):
            initial = billing_datum.start.date()
        if isinstance(billing_datum.end, datetime):
            closing = billing_datum.end.date()

        bills.append(
            Bill(
                service=service_oid,
                initial=initial,
                closing=closing,
                cost=billing_datum.cost,
                used=round(billing_datum.used, 4) if billing_datum.used else 0,
                peak=round(billing_datum.peak, 4) if billing_datum.peak else None,
                items=[i._asdict() for i in (billing_datum.items or [])],
                utility_code=billing_datum.utility_code,
                attachments=[
                    {
                        "key": attachment.key,
                        "kind": attachment.kind,
                        "format": attachment.format,
                    }
                    for attachment in (billing_datum.attachments or [])
                ],
                manual=False,
            ),
        )
    return bills


@deprecated(details="To be replaced by ORM module.")
def _upload_to_platform(service_id: str, billing_data: BillingData):
    bills = []
    for bill in billing_data:
        if not bill:
            continue

        bills.append(
            {
                "start": bill.start.strftime("%Y-%m-%d"),
                "end": bill.end.strftime("%Y-%m-%d"),
                "cost": str(bill.cost),
                "used": bill.used,
                "peak": bill.peak,
                "items": [
                    {
                        "description": item.description,
                        "quantity": item.quantity,
                        "rate": item.rate,
                        "total": item.total,
                        "kind": item.kind,
                        "unit": item.unit,
                    }
                    for item in (bill.items or [])
                ],
                "attachments": [
                    {
                        "key": attachment.key,
                        "kind": attachment.kind,
                        "format": attachment.format,
                    }
                    for attachment in (bill.attachments or [])
                ],
            }
        )

    log.info("Posting data to platform.")
    platform.post(
        "/object/utility-service/{}/bills/import".format(service_id),
        {"importance": "product", "bills": bills},
    )


def hash_bill(service_id, start_date, end_date, cost, demand, use):
    """Determine a key for the input bill_datum tuple that is unique (with high probability)."""
    fmt_string = "{0}_{1}_{2}_{3}_{4}_{5}"
    descriptor = fmt_string.format(
        service_id, start_date.isoformat(), end_date.isoformat(), cost, demand, use
    )
    return hashlib.sha224(descriptor.encode("utf-8")).hexdigest()


def hash_bill_datum(service_id: str, b: BillingDatum):
    return hash_bill(service_id, b.start, b.end, b.cost, b.peak, b.used)


def upload_bill_to_s3(
    file_handle: BytesLikeObject,
    key: str,
    source: str,
    statement: date,
    utility: str,
    utility_account_id: str,
    gen_utility: Optional[str] = None,
    gen_utility_account_id: Optional[str] = None,
) -> Optional[AttachmentEntry]:
    entry = AttachmentEntry(
        key=key,
        kind="bill",
        format="PDF",
        source=source,
        statement=statement.strftime("%Y-%m-%d"),
        utility=utility,
        utility_account_id=utility_account_id,
        gen_utility=gen_utility,
        gen_utility_account_id=gen_utility_account_id,
    )
    if s3_key_exists(config.BILL_PDF_S3_BUCKET, key):
        log.info("Bill %s already exists in S3. Skipping upload..." % key)
        BillDocument.add_or_update(entry)
        return entry

    try:
        upload_pdf_to_s3(file_handle, config.BILL_PDF_S3_BUCKET, key)
        BillDocument.add_or_update(entry)
    except:  # noqa E722
        log.exception("Failed to upload bill %s to S3.", key)
        return None

    return entry
