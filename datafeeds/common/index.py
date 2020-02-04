from datetime import datetime, date, timedelta
from typing import List, Optional
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
    IntervalRange,
    IntervalIssue,
    Status,
)

from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)


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
    "intervalFrom": {"type": "date"},
    "intervalTo": {"type": "date"},
    "runName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
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
        max_dt = max(max_dt, _get_date(doc["intervalTo"]))
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


def update_dates(
    task_id: str, billing: BillingRange = None, interval: IntervalRange = None
):
    """set date ranges retrieved by this run"""
    doc = {}
    if billing:
        doc.update({"billingFrom": billing.start, "billingTo": billing.end})
    if interval:
        doc.update({"intervalFrom": interval.start, "intervalTo": interval.end})
    if doc:
        index_etl_run(task_id, doc, update=True)


def update_billing_range(task_id: str, bills: BillingData):
    """set billing range retrieved by this run"""
    if not bills:
        return
    billing = BillingRange(
        start=min([bill.start for bill in bills]), end=max([bill.end for bill in bills])
    )
    update_dates(task_id, billing=billing)


def _meter_interval(meter_id):
    query = "select interval from meter where oid=:oid"
    result = db.session.execute(query, {"oid": meter_id}).fetchone()
    return result[0] if result else 15


@dbtask
def update_readings_range(task_id: str, meter_id: int, readings: dict):
    # noinspection SpellCheckingInspection
    """set readings range retrieved by this run

        readings data looks like 'yyyy-mdd-dd': [v1, v2, ...]
        get last valid readings date using meter interval
        """
    if not readings:
        return
    max_dt = max(readings.keys())  # this is a string
    data = readings[max_dt]
    # go backwards through the readings to find the last non-empty index
    for idx in range(len(data) - 1, -1, -1):
        if data[idx] is not None:  # 0 may be valid
            break
    missing = len(data) - idx
    interval = _meter_interval(meter_id)
    latest = (
        date_parser.parse(max_dt)
        + timedelta(days=1)
        - timedelta(minutes=interval * missing)
    )
    doc = {
        "intervalFrom": date_parser.parse(min(readings.keys())),
        "intervalTo": latest,
    }

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
