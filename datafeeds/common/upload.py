import logging
import csv
from datetime import timedelta, date, datetime

from deprecation import deprecated
import os
from typing import Optional, Union, BinaryIO, List
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
from datafeeds.models.bill import Bill, PartialBillProviderType
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


def upload_bills(
    meter_oid: int, service_id: str, task_id: str, billing_data: BillingData,
) -> Status:
    if config.enabled("PLATFORM_UPLOAD"):
        log.info("Uploading bills to platform via HTTP request.")
        _upload_to_platform(service_id, billing_data)

    if task_id and config.enabled("ES_INDEX_JOBS"):
        log.info("Updating billing range in Elasticsearch.")
        index.update_billing_range(task_id, meter_oid, billing_data)

    title = "Final Billing Summary"
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
    cur_most_recent = _latest_closing(service_id)
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
        index.set_interval_fields(task_id, meter_oid, updated)

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


def attach_bill_pdfs(meter_oid: int, task_id: str, pdfs: List[BillPdf],) -> Status:
    """Attach a list of bill PDF files uploaded to S3 to bill records."""
    if not pdfs:
        return Status.COMPLETED
    # A bill PDF is associated with one utility account; it can contain data for
    # multiple SAIDs.
    count = 0
    unused = []
    for pdf in pdfs:
        log.info(
            "bill PDF for utility_account_id=%s statement=%s",
            pdf.utility_account_id,
            pdf.statement,
        )
        # look for bill that ended recently before the statement date
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
            unused.append(pdf.s3_key)
        attached = False
        for bill in query:
            # [{"kind": "bill", "key": "1ea5a6c9-0a3c-d0fc-a0ba-0eae4f86ddeb.pdf", "format": "PDF"}]
            log.info(
                "matched bill pdf %s to bill %s %s-%s",
                pdf.statement,
                bill.oid,
                bill.initial,
                bill.closing,
            )
            current_pdfs = {
                att["key"]
                for att in bill.attachments or []
                if att.get("format") == "PDF"
            }
            # if bill already has a PDF, skip (may use a different name hash)
            if current_pdfs:
                if pdf.s3_key in current_pdfs:
                    attached = True
                continue
            attached = True
            if not bill.attachments:
                bill.attachments = []
            bill.attachments.append(
                {"kind": "bill", "key": pdf.s3_key, "format": "PDF"}
            )
            db.session.add(bill)
            log.info("adding attachment %s to bill %s", pdf.s3_key, bill.oid)
        if attached:
            count += 1
        else:
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
        index.update_billing_range(task_id, meter.oid, billing_data)

    return status


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
