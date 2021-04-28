from datetime import datetime, date
from typing import List, Set, Dict, Any, Tuple
import logging

from dateutil import parser as date_parser
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import bulk
from sqlalchemy.orm import joinedload

from datafeeds import db, config
from datafeeds.common.typing import (
    BillingData,
    BillingRange,
    IntervalIssue,
    BillPdf,
)

from datafeeds.models import (
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.models.meter import MeterReading
from datafeeds.models.user import SnapmeterUserSubscription, SnapmeterAccountUser

log = logging.getLogger(__name__)

INDEX = "etl-tasks"  # write alias
INDEX_PATTERN = "etl-tasks-*"
INTERVAL_ISSUE_INDEX = "etl-interval-issues"
BILLS_INDEX = "bills-log"
LOG_URL_PATTERN = "https://snapmeter.com/api/v2/admin/scraper-archive?id=%s"


def _get_es_connection():
    return Elasticsearch(
        [dict(host=config.ELASTICSEARCH_HOST, port=config.ELASTICSEARCH_PORT)],
        connection_class=RequestsHttpConnection,
        http_auth=(config.ELASTICSEARCH_USER, config.ELASTICSEARCH_PASSWORD),
        use_ssl=True,
    )


"""
    ES instance: https://6e4cab9dd2954f47a4a69440dc0247c0.us-east-1.aws.found.io:9243

    "time": {"type": "date"},
    "status": {"type": "keyword"},
    "error": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
    "accound": {"type": "keyword"},
    "account_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
    "meter": {"type": "keyword"},
    "meter_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
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


def get_index_doc(task_id: str) -> Tuple[Dict[str, Any], str]:
    es = _get_es_connection()
    # noinspection SpellCheckingInspection
    try:
        results = es.search(
            index=INDEX_PATTERN,
            doc_type="_doc",
            _source=True,
            body={"query": {"match": {"_id": task_id}}},
        )["hits"]["hits"]
        if not results:
            return {}, INDEX
        result = results[0]
        return result["_source"], result.get("_index", INDEX)
    except NotFoundError:
        return {}, INDEX


def index_etl_run(task_id: str, run: dict):
    """Index an ETL run: get the existing doc and update with fields in run."""
    es = _get_es_connection()
    doc, index = get_index_doc(task_id)
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
    es.index(index=INDEX, doc_type="_doc", id=task_id, body=doc, refresh="wait_for")


def index_bill_records(scraper: str, change_records: List[Dict[str, Any]]):
    """Index a list of bill change records."""
    # get meter log info for all referenced meters
    meter_log_extra: Dict[int, Dict] = {}
    for meter_id in {rec["meter"] for rec in change_records}:
        meter_log_extra[meter_id] = (
            db.session.query(Meter).get(meter_id).build_log_extra
        )
    records: List[Dict[str, Any]] = []
    for record in change_records:
        source = {
            "time": datetime.now(),
            "scraper": scraper,
            "operation": record["operation"],
        }
        source.update(meter_log_extra[record["meter"]])
        for field in record:
            # remove prefix
            if field.startswith("incoming_"):
                source[field.replace("incoming_", "")] = record[field]
            # collapse retained_ and replaced_ to previous_
            if field.startswith("retained_") or field.startswith("replaced_"):
                field_name = field.replace("retained_", "").replace("replaced_", "")
                source[f"prev_{field_name}"] = record[field]
        records.append({"_index": BILLS_INDEX, "_type": "_doc", "_source": source})
    bulk(_get_es_connection(), records)


def run_meta(meter_oid: int) -> Dict[str, Any]:
    """Set metadata common to all runs."""
    doc: Dict[str, Any] = {
        "emailSubscribers": SnapmeterUserSubscription.email_subscriber_count(meter_oid),
        "accountUsers": SnapmeterAccountUser.account_user_count(meter_oid),
    }
    meter = (
        db.session.query(Meter)
        .filter_by(oid=meter_oid)
        .options(joinedload(Meter.utility_service))
        .first()
    )
    if meter:
        doc.update(meter.build_log_extra)

    return doc


def starter_doc(meter_id: int, datasource: MeterDataSource) -> Dict[str, Any]:
    """Create a starter doc for indexing."""
    doc = run_meta(meter_id)
    doc.update(
        {
            "time": datetime.now(),
            "status": "STARTED",
            "scraper": datasource.name,
            "origin": "datafeeds",
            "started": datetime.now(),  # TODO: replaced by time; remove 2021-Q2
        }
    )
    return doc


def update_billing_range(task_id: str, bills: BillingData):
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
    index_etl_run(task_id, doc)


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
        start=min([bill.start for bill in pdfs]),
        end=max([bill.end for bill in pdfs]),
    )
    doc.update({"billingFrom": billing.start, "billingTo": billing.end})
    index_etl_run(task_id, doc)


def _meter_interval(meter_id):
    query = "select interval from meter where oid=:oid"
    result = db.session.execute(query, {"oid": meter_id}).fetchone()
    return result[0] if result else 15


def set_interval_fields(task_id: str, readings: List[MeterReading]):
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
    index_etl_run(task_id, doc)


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
                "time": datetime.now(),
                "intervalDateTime": issue.interval_dt,
                "error": issue.error,
                "value": issue.value,
                "account": account_hex,
                "account_name": account_name,
                "meter": meter_id,
                "meter_name": meter_name,
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


def index_logs(task_id: str):
    """Upload the logs for this task to elasticsearch for later analysis."""
    try:
        with open(config.LOGPATH, "r") as f:
            log_contents = f.read()
        doc = {"log": log_contents}
        index_etl_run(task_id, doc)
    except:  # noqa E722
        log.exception("Failed to upload run logs to elasticsearch.")
        return

    log.info("Successfully uploaded run logs to elasticsearch.")
