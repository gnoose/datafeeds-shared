import json
import logging
import csv
from deprecation import deprecated
import os
from typing import Optional, Union, BinaryIO, List
from io import BytesIO
import hashlib


from datafeeds import config
from datafeeds.common import webapps, index, platform
from datafeeds.common.typing import (
    BillingData,
    show_bill_summary,
    BillingDatum,
    AttachmentEntry,
    BillPdf,
)
from datafeeds.common import interval_transform
from datafeeds.common.util.s3 import upload_pdf_to_s3
from datafeeds.common.util.s3 import s3_key_exists


BytesLikeObject = Union[BinaryIO, BytesIO]

log = logging.getLogger(__name__)

UPLOAD_DATA_BATCH_SIZE = 20


def upload_bills(service_id: str, task_id: str, billing_data: BillingData):
    if config.enabled("PLATFORM_UPLOAD"):
        log.info("Uploading bills to platform via HTTP request.")
        _upload_to_platform(service_id, billing_data)

    if task_id and config.enabled("ES_INDEX_JOBS"):
        log.info("Updating billing range in Elasticsearch.")
        index.update_billing_range(task_id, billing_data)

    title = "Final Billing Summary"
    show_bill_summary(billing_data, title)

    path = os.path.join(config.WORKING_DIRECTORY, "bills.csv")
    with open(path, "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Service ID", "Start", "End", "Cost", "Used", "Peak"])
        for b in billing_data:
            writer.writerow([service_id, b.start, b.end, b.cost, b.used, b.peak])
    log.info("Wrote bill data to %s." % path)


def upload_readings(
    transforms,
    task_id: str,
    meter_oid: int,
    account_hex_id: str,
    scraper: str,
    readings,
):
    if readings and config.enabled("PLATFORM_UPLOAD"):
        readings = interval_transform.transform(
            transforms, task_id, scraper, meter_oid, readings
        )
        _upload_via_webapps(readings, account_hex_id, meter_oid)

    if task_id and config.enabled("ES_INDEX_JOBS"):
        index.update_readings_range(task_id, meter_oid, readings)

    log.info("Final Interval Summary")
    for when, intervals in readings.items():
        log.info("%s: %s intervals." % (when, len(intervals)))

    path = os.path.join(config.WORKING_DIRECTORY, "readings.csv")
    with open(path, "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Service", "Date", "Readings"])
        for when, intervals in readings.items():
            writer.writerow([meter_oid, str(when)] + [str(x) for x in intervals])
    log.info("Wrote interval data to %s." % path)


def attach_bill_pdfs(
    task_id: str, pdfs: List[BillPdf],
):
    """POST a list of bill PDF files uploaded to S3 to webapps."""
    if not pdfs:
        return
    log.info("posting %s bills pdfs to webapps", len(pdfs))
    webapps.post("/api/v2/attach-bill-pdfs", {"pdfs": [pdf.to_json() for pdf in pdfs]})
    if task_id and config.enabled("ES_INDEX_JOBS"):
        log.info("Updating billing range in Elasticsearch.")
        index.update_bill_pdf_range(task_id, pdfs)


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


@deprecated(details="To be replaced by ORM module.")
def _upload_via_webapps(data, account_id, meter_id, dst_strategy="none"):
    """Upload formatted interval data to platform. This handles batching
    the upload to maximize efficiency and general error handling around it.
    The interval format is:

    {
        '%Y-%m-%d': [96]
    }

    IE:
    {
        '2017-04-02' : [59.1, 30.2,...]
    }
    """

    data_to_upload = {}
    batch_number = 0
    response = webapps.post("/transactions/create", {"target": meter_id})
    transaction_oid = response["oid"]

    log.debug("Opened stasis transaction. Transaction OID: %s.", transaction_oid)

    for key in data.keys():
        data_to_upload[key] = data[key]
        if len(data_to_upload) == UPLOAD_DATA_BATCH_SIZE:
            log.debug(
                "Uploading %d-%d of %d"
                % (
                    batch_number * UPLOAD_DATA_BATCH_SIZE,
                    (batch_number * UPLOAD_DATA_BATCH_SIZE) + UPLOAD_DATA_BATCH_SIZE,
                    len(data),
                )
            )

            webapps.post(
                "/accounts/%s/meters/%s/readings" % (account_id, meter_id),
                dict(
                    transaction=transaction_oid,
                    readings=json.dumps(data_to_upload),
                    dstStrategy=dst_strategy,
                ),
            )

            data_to_upload = {}
            batch_number += 1

    if data_to_upload:
        log.debug("Uploading last data batch.")
        webapps.post(
            "/accounts/%s/meters/%s/readings" % (account_id, meter_id),
            dict(
                transaction=transaction_oid,
                readings=json.dumps(data_to_upload),
                dstStrategy=dst_strategy,
            ),
        )

    webapps.post("/transactions/commit", {"oid": transaction_oid})
    log.debug("Committed stasis transaction.")


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
    file_handle: BytesLikeObject, key: str
) -> Optional[AttachmentEntry]:
    entry = AttachmentEntry(key=key, kind="bill", format="PDF")
    if s3_key_exists(config.BILL_PDF_S3_BUCKET, key):
        log.info("Bill %s already exists in S3. Skipping upload..." % key)
        return entry

    try:
        upload_pdf_to_s3(file_handle, config.BILL_PDF_S3_BUCKET, key)
    except:  # noqa E722
        log.exception("Failed to upload bill %s to S3.", key)
        return None

    return entry
