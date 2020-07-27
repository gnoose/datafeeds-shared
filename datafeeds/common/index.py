from datetime import datetime, date
from typing import List, Optional, Set, Dict, Any
import logging

from dateutil import parser as date_parser
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import bulk

from datafeeds import db, config
from datafeeds.db import dbtask
from datafeeds.common.typing import (
    BillingData,
    BillingRange,
    IntervalIssue,
    Status,
    BillPdf,
)

from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.models.meter import MeterReading
from datafeeds.models.user import SnapmeterUserSubscription, SnapmeterAccountUser

log = logging.getLogger(__name__)

INDEX = "etl-tasks"  # alias to indexes named etl-tasks-2019.05.15-1
INTERVAL_ISSUE_INDEX = "etl-interval-issues"
LOG_URL_PATTERN = "https://snapmeter.com/api/admin/etl-tasks/%s/log"


def _get_es_connection():
    return Elasticsearch(
        [dict(host=config.ELASTICSEARCH_HOST, port=config.ELASTICSEARCH_PORT)],
        connection_class=RequestsHttpConnection,
        http_auth=(config.ELASTICSEARCH_USER, config.ELASTICSEARCH_PASSWORD),
        use_ssl=True,
    )


"""
    ES instance: https://6e4cab9dd2954f47a4a69440dc0247c0.us-east-1.aws.found.io:9243

    "started": {"type": "date"},
    "status": {"type": "keyword"},
    "error": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
    "accountId": {"type": "keyword"},
    "accountName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
    "meterId": {"type": "keyword"},
    "meterName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
    "scraper": {"type": "keyword"},
    "url": {"type": "keyword"},
    "billingFrom": {"type": "date"},
    "billingTo": {"type": "date"},
    "intervalFrom": {"type": "date"}, - date range of non-null new/updated data
    "intervalTo": {"type": "date"},
    "runName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
    "billScraper": {"type": "boolean"}
    "intervalScraper": {"type": "boolean"}
    "updatedDays": {"type": "long"} - count of days containing updated data
    "weeklyEmailSubscribers": {"type": "long"} - count of external weekly email subscribers for this meter
    "accountUsers": {"type": "long"} - count of external users for this account
    "age": {"type": "long"} - days between max updated data and now
    "exception": {"type": "keyword"} - just the exception type
"""


def _get_date(dt):
    if isinstance(dt, str):
        return date_parser.parse(dt)
    return dt


def index_etl_run(task_id: str, run: dict, update: bool = False):
    """index an ETL run; get and update existing run if specified

    Doc fields: started, status, error, accountId, accountName, meterId, meterName, scraper
    Required for new record: started, status, meterId, scraper
    Retry up to 5 times if there's an error reaching the index.
    """
    if not update:
        for field in ["started", "status", "meterId", "scraper"]:
            if field not in run:
                log.error("missing field %s when indexing %s", field, task_id)
                return
    es = _get_es_connection()
    doc = {}
    if update:
        # noinspection SpellCheckingInspection
        try:
            task = es.get(index=INDEX, doc_type="_doc", id=task_id, _source=True)
            doc = task["_source"]
        except NotFoundError:
            log.error("update of task %s failed: not found", task_id)
            return
    doc.update(run)
    doc["updated"] = datetime.now()
    doc["url"] = LOG_URL_PATTERN % task_id
    # set latestFetched from max of billing/interval
    min_dt = datetime(2000, 1, 1)
    max_dt = datetime(2000, 1, 1)
    if doc.get("intervalTo"):
        interval_to = _get_date(doc["intervalTo"])
        if isinstance(interval_to, date):
            interval_to = datetime.combine(interval_to, datetime.min.time())
        max_dt = max(max_dt, interval_to)
    if doc.get("billingTo"):
        billing_to = _get_date(doc["billingTo"])
        if isinstance(billing_to, date):
            billing_to = datetime.combine(billing_to, datetime.min.time())
        max_dt = max(max_dt, billing_to)
    if max_dt > min_dt:
        doc["maxFetched"] = max_dt
    log.debug(
        "Transmitted to Elasticsearch: task_id=%s, update=%s, document=%s",
        task_id,
        update,
        doc,
    )
    es.index(index=INDEX, doc_type="_doc", id=task_id, body=doc)


def run_meta(meter_oid: int) -> Dict[str, Any]:
    """Set metadata common to all runs."""
    doc: Dict[str, Any] = {
        "emailSubscribers": SnapmeterUserSubscription.email_subscriber_count(meter_oid),
        "accountUsers": SnapmeterAccountUser.account_user_count(meter_oid),
    }
    return doc


def update_billing_range(task_id: str, meter_oid: int, bills: BillingData):
    """Index info about the data retrieved in this run.

    dataType - bill
    weeklyEmailSubscribers - count of external weekly email subscribers for this meter
    accountUsers - count of external users for this account
    billingFrom, billingTo - date range for bills retrieved during this run (all bills, not just updated)
    """
    if not bills:
        return
    doc: Dict[str, Any] = {}
    billing = BillingRange(
        start=min([bill.start for bill in bills]),
        end=max([bill.end for bill in bills]),
    )
    doc.update({"billingFrom": billing.start, "billingTo": billing.end})
    index_etl_run(task_id, doc, update=True)


def update_bill_pdf_range(task_id: str, meter_oid: int, pdfs: List[BillPdf]):
    """Index info about the data retrieved in this run.

    dataType - bill
    weeklyEmailSubscribers - count of external weekly email subscribers for this meter
    accountUsers - count of external users for this account
    billingFrom, billingTo - date range for bills retrieved during this run (all bills, not just updated)
    """
    if not pdfs:
        return
    doc: Dict[str, Any] = {}
    billing = BillingRange(
        start=min([bill.start for bill in pdfs]), end=max([bill.end for bill in pdfs]),
    )
    doc.update({"billingFrom": billing.start, "billingTo": billing.end})
    index_etl_run(task_id, doc, update=True)


def _meter_interval(meter_id):
    query = "select interval from meter where oid=:oid"
    result = db.session.execute(query, {"oid": meter_id}).fetchone()
    return result[0] if result else 15


@dbtask
def set_interval_fields(task_id: str, meter_oid: int, readings: List[MeterReading]):
    """Index info about the interval data retrieved in this run.

    dataType - interval
    intervalUpdatedFrom, intervalUpdatedTo - date range of non-null new/updated data
    updatedDays - count of days containing updated data
    weeklyEmailSubscribers - count of external weekly email subscribers for this meter
    accountUsers - count of external users for this account
    age - days between max updated data and now
    """
    doc: Dict[str, Any] = {}
    dates: Set[date] = set()
    for reading in readings or []:
        values = set(reading.readings)
        if not values == {None}:
            dates.add(reading.occurred)
    doc["updatedDays"] = len(dates)
    if dates:
        doc.update(
            {
                "intervalFrom": min(dates),
                "intervalTo": max(dates),
                "age": (date.today() - max(dates)).days,
            }
        )
    index_etl_run(task_id, doc, update=True)


def _interval_issues_docs(
    task_id: str,
    account_hex: str,
    account_name: str,
    meter_id: int,
    meter_name: str,
    scraper: str,
    issues: List[IntervalIssue],
):
    for issue in issues:
        yield {
            "_index": INTERVAL_ISSUE_INDEX,
            "_type": "_doc",
            "_id": "%s-%s-%s"
            % (task_id, meter_id, issue.interval_dt.strftime("%Y%m%d%H%M")),
            "_source": {
                "updated": datetime.now(),
                "intervalDateTime": issue.interval_dt,
                "error": issue.error,
                "value": issue.value,
                "accountId": account_hex,
                "accountName": account_name,
                "meterId": meter_id,
                "meterName": meter_name,
                "scraper": scraper,
            },
        }


def index_etl_interval_issues(
    task_id: str,
    account_hex: str,
    account_name: str,
    meter_id: int,
    meter_name: str,
    scraper: str,
    issues: List[IntervalIssue],
):
    """Index a list of interval data issues.

    Doc fields: intervalDateTime, error, accountId, accountName, meterId, meterName, scraper, value.
    """
    log.info(
        "indexing %s interval issues for meter %s account %s",
        len(issues),
        meter_id,
        account_name,
    )
    bulk(
        _get_es_connection(),
        _interval_issues_docs(
            task_id, account_hex, account_name, meter_id, meter_name, scraper, issues
        ),
    )


def index_logs(
    task_id: str,
    acct: Optional[SnapmeterAccount],
    meter: Meter,
    ds: MeterDataSource,
    status: Status,
):
    """Upload the logs for this task to elasticsearch for later analysis."""
    es = _get_es_connection()

    try:
        # Try to acquire a copy of the existing document created for this run.
        task = es.get(index=INDEX, doc_type="_doc", id=task_id, _source=True)
        doc = task["_source"]
    except NotFoundError:
        # Make a document with fundamental information about the run.
        doc = dict(
            meterId=meter.oid,
            meterName=meter.name,
            uploaded=datetime.now(),
            scraper=ds.name,
            status=str(status.name),
        )

        if acct is not None:
            doc["accountId"] = acct.hex_id
            doc["accountName"] = acct.name

    try:
        with open(config.LOGPATH, "r") as f:
            log_contents = f.read()
        doc["log"] = log_contents
        es.index(INDEX, doc_type="_doc", id=task_id, body=doc)
    except:  # noqa E722
        log.exception("Failed to upload run logs to elasticsearch.")
        return

    log.info("Successfully uploaded run logs to elasticsearch.")
