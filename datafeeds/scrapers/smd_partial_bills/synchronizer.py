import logging
from typing import Optional, Set, List

from datafeeds import db
from datafeeds.common import Configuration, Results, BaseApiScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    Meter,
    SnapmeterAccount,
    SnapmeterMeterDataSource as MeterDataSource,
)

from datafeeds.scrapers.smd_partial_bills.models import Bill as SmdBill, CustomerInfo


log = logging.getLogger(__name__)


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

    records = db.session.query(CustomerInfo).filter(
        CustomerInfo.service_id == us.service_id.strip()
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

    @property
    def service(self):
        meter = self._configuration.meter
        return meter.utility_service

    def _execute(self):
        config: SmdPartialBillingScraperConfiguration = self._configuration
        meter = config.meter

        usage_points = relevant_usage_points(meter)
        log.info(
            "Identified %s relevant usage point(s): %s", len(usage_points), usage_points
        )
        query = db.session.query(SmdBill).filter(SmdBill.usage_point.in_(usage_points))

        if self.start_date:
            query = query.filter(self.start_date <= SmdBill.start)

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
