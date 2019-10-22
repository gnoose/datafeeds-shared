import json
import logging

from deprecation import deprecated

from datafeeds import config
from datafeeds.common import webapps, index, platform
from datafeeds.common.typing import BillingData, show_bill_summary
from datafeeds.common import interval_transform

log = logging.getLogger(__name__)

UPLOAD_DATA_BATCH_SIZE = 20


def upload_bills(utility_service_oid: int, task_id: str, billing_data: BillingData):
    if config.enabled("PLATFORM_UPLOAD"):
        log.info("Uploading bills to platform via HTTP request.")
        _upload_to_platform(utility_service_oid, billing_data)

    if task_id and config.enabled("ES_INDEX_JOBS"):
        log.info("Updating billing range in Elasticsearch.")
        index.update_billing_range(task_id, billing_data)

    title = "Final Billing Summary"
    show_bill_summary(billing_data, title)


def upload_readings(transforms, task_id: str, meter_oid: int, account_hex_id: str, scraper: str, readings):
    if transforms and readings and config.enabled("PLATFORM_UPLOAD"):
        readings = interval_transform.transform(transforms, task_id, scraper, meter_oid, readings)
        _upload_via_webapps(readings, account_hex_id, meter_oid)

    if task_id and config.enabled("ES_INDEX_JOBS"):
        index.update_readings_range(task_id, meter_oid, readings)

    log.info("Final Interval Summary")
    for when, intervals in readings.items():
        log.info("%s: %s intervals." % (when, len(intervals)))
    return


@deprecated(details="To be replaced by ORM module.")
def _upload_to_platform(utility_service_oid: str, billing_data: BillingData):
    bills = []
    for bill in billing_data:
        if not bill:
            continue
        bills.append({
            "start": bill.start.strftime("%Y-%m-%d"),
            "end": bill.end.strftime("%Y-%m-%d"),
            "cost": str(bill.cost),
            "used": str(bill.used) if bill.used else "0.0",
            "peak": str(bill.peak) if bill.peak else "0.0",
            "items": bill.items or [],
            "attachments": bill.attachments or []
        })

    log.info("Posting data to platform.")
    platform.post(
        "/object/utility-service/{}/bills/import".format(utility_service_oid),
        {"bills": bills}
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

    for key in data.keys():
        data_to_upload[key] = data[key]
        if len(data_to_upload) == UPLOAD_DATA_BATCH_SIZE:
            log.debug(
                "Uploading %d-%d of %d" % (
                    batch_number * UPLOAD_DATA_BATCH_SIZE,
                    (batch_number * UPLOAD_DATA_BATCH_SIZE) + UPLOAD_DATA_BATCH_SIZE,
                    len(data)
                )
            )

            webapps.post(
                "/accounts/%s/meters/%s/readings" % (account_id, meter_id),
                dict(
                    transaction=transaction_oid,
                    readings=json.dumps(data_to_upload),
                    dstStrategy=dst_strategy
                )
            )

            data_to_upload = {}
            batch_number += 1

    if data_to_upload:
        log.debug("Uploading last data batch")
        webapps.post(
            "/accounts/%s/meters/%s/readings" % (account_id, meter_id),
            dict(
                transaction=transaction_oid,
                readings=json.dumps(data_to_upload),
                dstStrategy=dst_strategy
            )
        )

    webapps.post("/transactions/commit", {"oid": transaction_oid})
