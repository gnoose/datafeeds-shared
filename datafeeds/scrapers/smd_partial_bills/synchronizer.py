import logging
from datetime import timedelta, date
from typing import Optional, Set, List

from pymysql.cursors import DictCursor
from sqlalchemy import distinct

from datafeeds import db, config as datafeeds_config
from datafeeds.common.base import BaseApiScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status, BillingData
from datafeeds.models import (
    Meter,
    SnapmeterAccount,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.models.utility_service import UtilityServiceSnapshot, UtilityService

from datafeeds.scrapers.smd_partial_bills.models import Bill as SmdBill, CustomerInfo
from datafeeds.urjanet.datasource.pymysql_adapter import (
    create_placeholders,
    SqlRowDict,
)
from datafeeds.urjanet.scraper import make_attachments

log = logging.getLogger(__name__)


def get_service_ids(us: UtilityService) -> List[str]:
    """Return related service ids from UtilityServiceSnapshots
    """
    service_ids = [
        said[0].strip()
        for said in db.session.query(
            distinct(UtilityServiceSnapshot.service_id)
        ).filter(
            UtilityServiceSnapshot.service == us.oid,
            UtilityServiceSnapshot.service_id.isnot(None),
        )
    ]
    if us.service_id not in service_ids:
        # This *should* be in the snapshot table, but this is helpful for testing
        # and covering our bases.
        service_ids.append(us.service_id.strip())
    return service_ids


def relevant_usage_points(m: Meter) -> Set[str]:
    """Compute a list of usage points associated with this meter.

    A valid usage point is any of the following:
    - The "stored usage point" associated with this meters meter data source.
        (This is likely to be invariant under SAID changes, which is why we keep it.)
    - Any usage point related to the meter's current SAID in the customer info table.

    If the current meter data source has no usage point associated with it,
    this function will assign one for future use.
    """

    us = m.utility_service
    if us is None:
        return set()

    service_ids = get_service_ids(us)

    records = db.session.query(CustomerInfo).filter(
        CustomerInfo.service_id.in_(service_ids)
    )

    usage_points = {rec.usage_point for rec in records}

    mds = (
        db.session.query(MeterDataSource)
        .filter(MeterDataSource.meter == m, MeterDataSource.name == "share-my-data")
        .first()
    )

    if not mds:
        return usage_points

    if mds.meta is None:
        mds.meta = {}

    stored_point = mds.meta.get("usage_point")
    if stored_point is not None:
        usage_points.add(stored_point)
    elif usage_points:
        mds.meta["usage_point"] = next(
            iter(usage_points)
        )  # No usage point currently assigned, so assign one.
        db.session.add(mds)

    return set(usage_points)


class SmdPartialBillingScraperConfiguration(Configuration):
    def __init__(self, meter: Meter):
        super().__init__(
            scrape_bills=False, scrape_readings=False, scrape_partial_bills=True,
        )
        self.meter = meter


class SmdPartialBillingScraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "SMD Partial Billing Synchronizer"
        self.conn = None

    @property
    def service(self):
        meter = self._configuration.meter
        return meter.utility_service

    def fetch_one(self, query: str, *argv) -> SqlRowDict:
        """Helper function for executing a query and fetching a single result"""
        with self.conn.cursor(DictCursor) as cursor:
            cursor.execute(query, tuple(argv))
            return cursor.fetchone()

    def attach_corresponding_urja_pdfs(self, partial_bills: BillingData) -> BillingData:
        """Attempt to update each SMD Partial Bill with the latest statement from Urjanet.
        """
        self.conn = db.urjanet_connection()
        utility_account_id = self.service.utility_account_id
        service_ids = get_service_ids(self.service)
        query = """
            SELECT xmlaccount.SourceLink, xmlaccount.StatementDate
            FROM xmlaccount, xmlmeter
            WHERE xmlaccount.PK = xmlmeter.AccountFK
                AND xmlaccount.UtilityProvider = 'PacGAndE'
                AND RawAccountNumber = %s
                AND PODid in ({})
                AND xmlmeter.IntervalStart > %s
                AND xmlmeter.IntervalStart < %s
            ORDER BY xmlaccount.StatementDate DESC
            LIMIT 1
        """.format(
            create_placeholders(service_ids)
        )

        updated_partials = []
        for pb in partial_bills:
            attachments = None

            pdf = self.fetch_one(
                query,
                utility_account_id,
                *service_ids,
                pb.start - timedelta(days=1),
                pb.start + timedelta(days=1)
            )

            if pdf:
                source_url = pdf.get("SourceLink")
                statement = pdf.get("StatementDate", pb.statement or pb.end)

                attachments = make_attachments(
                    source_urls=[source_url],
                    statement=statement,
                    utility=self.service.utility,
                    account_id=utility_account_id,
                    gen_utility=self.service.gen_utility,
                    gen_utility_account_id=self.service.gen_utility_account_id,
                )

            if attachments:
                updated_partials.append(pb._replace(attachments=attachments))
            else:
                updated_partials.append(pb)
        self.conn.close()
        return updated_partials

    def _execute(self):
        config: SmdPartialBillingScraperConfiguration = self._configuration
        meter = config.meter

        usage_points = relevant_usage_points(meter)
        log.info(
            "Identified %s relevant usage point(s): %s", len(usage_points), usage_points
        )
        query = db.session.query(SmdBill).filter(SmdBill.usage_point.in_(usage_points))

        if self.start_date:
            start = self.start_date
            end = max(start, self.end_date or date.today())
            if end - self.start_date <= timedelta(days=60):
                start = start - timedelta(days=60)
                log.info("Adjusting start date to %s.", start)
            query = query.filter(start <= SmdBill.start)

        if self.end_date:
            query = query.filter(SmdBill.start <= self.end_date)

        query = query.order_by(SmdBill.published)

        log.info("Identified %d raw SMD bills relevant to this meter.", query.count())
        # It often happens that we receive several versions of the same bill across multiple files.
        # The first thing we need to do is order the bills by publication date, so we can decide
        # which SmdBill record is the correct one for our chosen date.
        unified_bills: List[SmdBill] = SmdBill.unify_bills(query)
        partial_bills = [b.to_billing_datum(self.service) for b in unified_bills]

        if partial_bills:
            log.debug(
                "Identified %s partial bills in Share My Data for meter %s (%s).",
                len(partial_bills),
                meter.name,
                meter.oid,
            )
            if datafeeds_config.enabled("S3_BILL_UPLOAD"):
                partial_bills = self.attach_corresponding_urja_pdfs(partial_bills)

        return Results(tnd_bills=partial_bills)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SmdPartialBillingScraperConfiguration(meter)
    return run_datafeed(
        SmdPartialBillingScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
        disable_login_on_error=True,
    )
