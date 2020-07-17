from datetime import datetime, date, timedelta
import logging
from typing import Optional, List

from dateutil.relativedelta import relativedelta
from dateutil.tz import gettz as get_timezone, tzutc
import requests

from jsonobject import (
    JsonObject,
    StringProperty,
    DateTimeProperty,
    FloatProperty,
    ListProperty,
)

from datafeeds import config
from datafeeds.common import (
    BillingDatum,
    Configuration as BaseConfiguration,
    DateRange,
    Results,
    BaseApiScraper,
    adjust_bill_dates,
)
from datafeeds.common.batch import run_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)


log = logging.getLogger(__name__)


class IngestApiError(Exception):
    pass


class SearchSpecification(JsonObject):
    subscription = StringProperty(required=True)
    usage_point = StringProperty(required=True)
    start = DateTimeProperty(required=True)
    end = DateTimeProperty(required=True)


class CostDetail(JsonObject):
    note = StringProperty()
    amount = FloatProperty()


class Bill(JsonObject):
    total = FloatProperty()
    use = FloatProperty()
    unit = StringProperty()
    start = DateTimeProperty()
    end = DateTimeProperty()
    details = ListProperty(CostDetail)


class IntervalDatum(JsonObject):
    value = FloatProperty()
    start = DateTimeProperty()
    end = DateTimeProperty()


class LoadCurve(JsonObject):
    values = ListProperty(IntervalDatum)
    unit = StringProperty()
    flow_direction = StringProperty()


class CustomerInformation(JsonObject):
    state_change = StringProperty()  # Can be Enrollment, Unenrollment, or Meter Change

    account_group_identifier = StringProperty()
    account_identifier = StringProperty()  # Account Number
    service_identifier = StringProperty()  # Meter Number

    email = StringProperty()
    termination_date = DateTimeProperty()
    tariff = StringProperty()


class ServiceUpdate(JsonObject):
    load_curves = ListProperty(LoadCurve)
    bills = ListProperty(Bill)
    customer_information = ListProperty(CustomerInformation)
    subscription = StringProperty()
    usage_point = StringProperty()


class Configuration(BaseConfiguration):
    def __init__(self, subscription, usage_point):
        super().__init__(
            scrape_readings=True, scrape_bills=True
        )  # Interval readings to be supported at a later date.
        self.subscription = subscription
        self.usage_point = usage_point


utc_tz = tzutc()
ca_tz = get_timezone("America/Los_Angeles")


def _adjust_timezone(dt):
    """Convert a datetime-naive UTC timestamp to California time. (Local time for all SCE meters.)"""
    dt_utc = dt.replace(tzinfo=utc_tz)
    return dt_utc.astimezone(ca_tz)


SIDEBAR_ITEMS = {
    "Distribution Charge",
    "Franchise Fee",
    "Nuclear Decommission Charge",
    "Nuclear Decommissioning Charges",
    "Public Purpose Program Charge",
    "Public Purpose Programs Charge",
    "Transmission Charge",
    "Transmission Charges",
}


def process_bill(b: Bill) -> Optional[Bill]:
    if b.total is not None:
        # SCE sent us a bill with a total, so just use the bill as-is.
        return b

    if not b.details:
        # No explicit total from SCE, and no line items. We can't recover a bill.
        return None

    # No bill total, but we can try to recover the total for the bill from the line items.
    return Bill(
        total=sum(
            d.amount
            for d in b.details
            if d.note not in SIDEBAR_ITEMS and d.amount is not None
        ),
        use=b.use if b.use is not None and b.use > 0 else None,
        unit=b.unit,
        start=b.start,
        end=b.end,
        details=b.details,
    )


def correct_bills(bills: List[BillingDatum]):
    # SCE sent us a large amount of bad bill information where the cost is the same as a more
    # recent bill b and the use value is 100x the use listed on b. We have to exclude these
    # with custom logic since SCE will not correct the problem.
    observed_cost_use_pairs = set(
        (b.cost, round(b.used, 2)) for b in bills if b.used is not None and b.used > 0
    )

    return [
        b
        for b in bills
        if b.used is None
        or (b.cost, round(b.used / 100, 2)) not in observed_cost_use_pairs
    ]


class Scraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "SCE Green Button Synchronizer"

    @property
    def subscription(self):
        return self._configuration.subscription

    @property
    def usage_point(self):
        return self._configuration.usage_point

    def _execute(self):
        root = config.INGEST_ENDPOINT

        # SCE Green Button does not offer data earlier than 2015.
        # Truncate so that we don't make trivial requests.
        start = max(self.start_date, date(2015, 1, 1))
        end = min(self.end_date, date.today())

        # SCE sometimes does not publish the most recent bill until several weeks after its close date.
        # Ensure the time window is large enough to capture some bills.
        if end - start < timedelta(days=90):
            start = end - timedelta(days=90)

        date_range = DateRange(start, end)

        service_updates = []
        for interval in date_range.split_iter(relativedelta(months=1)):
            start_dt = datetime(
                interval.start_date.year,
                interval.start_date.month,
                interval.start_date.day,
            )
            end_dt = datetime(
                interval.end_date.year, interval.end_date.month, interval.end_date.day
            )
            body = SearchSpecification(
                subscription=self.subscription,
                usage_point=self.usage_point,
                start=start_dt,
                end=end_dt,
            )
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                root + "/sce/search", json=body.to_json(), headers=headers
            )
            if response.status_code != 200:
                log.error("API request to Ingest failed.")
                raise IngestApiError()  # Fail this scraper run.

            service_updates.append(ServiceUpdate(response.json()))

        # # SCE GB: Enable interval data.
        # load_curves = [lc for su in service_updates for lc in su.load_curves]
        # interval = determine_interval(load_curves)
        # timeline = Timeline(start, end, interval=interval)
        bills = set()
        for su in service_updates:
            # # SCE GB: Enable interval data.
            # for curve in su.load_curves:
            #     multiplier = determine_demand_multiplier(curve.unit, interval)
            #     for v in curve.values:
            #         timeline.insert(_adjust_timezone(v.start), multiplier * v.value)

            for b in su.bills:
                processed = process_bill(b)
                if processed is None:
                    log.info("Skipping bill with null use: %s" % b)
                    continue

                if processed.start > processed.end:
                    log.info("Skipping bill with start after end: %s")
                    continue

                use = (
                    processed.use / 1000
                    if processed.unit == "Wh" and processed.use is not None
                    else processed.use
                )

                bills.add(
                    BillingDatum(
                        start=_adjust_timezone(processed.start).date(),
                        end=_adjust_timezone(processed.end).date(),
                        statement=_adjust_timezone(processed.end).date(),
                        cost=processed.total,
                        used=use,
                        peak=None,  # Peak, line items, and attachments aren't available.
                        items=None,
                        attachments=None,
                    )
                )

        corrected_bills = correct_bills(bills)

        # If bills arrived in a large historical block, ingest will return the block.
        # Filter the bills for just those whose start date appears in the scraped time period.
        filtered_bills = [b for b in corrected_bills if start <= b.start <= end]
        adjusted_bills = adjust_bill_dates(filtered_bills)
        final_bills = [b for b in adjusted_bills if b.start <= b.end]

        return Results(
            # readings=timeline.serialize(),  # SCE GB: Enable interval data.
            bills=final_bills
        )


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
):

    if not ("subscription" in datasource.meta and "usage_point" in datasource.meta):
        msg = (
            "No subscription/usage point pair associated with data source. Skipping. (Data Source OID: %s)"
            % datasource.oid
        )
        log.info(msg)

        # Eventually this will be a genuine failure condition, but until we can completely convert to
        # ingest-based SCE green button, we need to just skip when the data isn't available.
        # raise DataSourceConfigurationError(msg)
        return Status.SKIPPED

    return run_datafeed(
        Scraper,
        account,
        meter,
        datasource,
        params,
        configuration=Configuration(
            subscription=datasource.meta["subscription"],
            usage_point=datasource.meta["usage_point"],
        ),
        task_id=task_id,
    )
