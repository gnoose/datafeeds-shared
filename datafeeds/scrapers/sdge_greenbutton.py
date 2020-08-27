from typing import List, Optional
from datetime import datetime, date, timedelta
import logging

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc, gettz
import requests
from jsonobject import (
    JsonObject,
    StringProperty,
    DateTimeProperty,
    FloatProperty,
    ListProperty,
)

from datafeeds import config
from datafeeds.common import BillingDatum, Configuration, DateRange, Results
from datafeeds.common.base import BaseApiScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.timeline import Timeline
from datafeeds.common.typing import Status, adjust_bill_dates
from datafeeds.models import Meter, SnapmeterAccount, SnapmeterMeterDataSource

log = logging.getLogger(__name__)


class InvalidIntervalException(Exception):
    pass


class SearchSpecification(JsonObject):
    usage_point = StringProperty(required=True)
    start = DateTimeProperty(required=True)
    end = DateTimeProperty(required=True)


class Bill(JsonObject):
    total = FloatProperty()
    use = FloatProperty()
    unit = StringProperty()
    start = DateTimeProperty()
    end = DateTimeProperty()


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


class SdgeGreenButtonAccountConfiguration(Configuration):
    def __init__(self, usage_point):
        super().__init__(scrape_readings=True, scrape_bills=True)
        self.usage_point = usage_point


def determine_interval(curves: List[LoadCurve]) -> int:
    deltas = set(
        int((i.end - i.start).total_seconds() / 60) for c in curves for i in c.values
    )

    if not deltas:
        log.info("Using default interval size of 15 minutes.")
        return 15  # Since curves is empty, the interval we choose is irrelevant, so just pick the default.

    if len(deltas) == 1:
        ivl = deltas.pop()
        log.info("Using interval size: %s minutes.", ivl)
        return ivl

    raise InvalidIntervalException("Detected multiple interval sizes for this meter.")


UTC = tzutc()
CA_TZ = gettz("America/Los_Angeles")


def _adjust_timezone(dt):
    """Convert a datetime-naive UTC timestamp to California time. (Local time for all SDGE meters.)"""
    dt_utc = dt.replace(tzinfo=UTC)
    return dt_utc.astimezone(CA_TZ)


def determine_demand_multiplier(unit: str, interval: int):
    lookup = {
        "Wh": (60 / interval) * 0.001,  # Convert Wh to kW
        "W": 0.001,  # Convert to kW
        "therm": 1.0,
    }

    return lookup.get(unit, 1.0)


class SdgeGreenButtonScraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "SDG&E Green Button Synchronizer"

    @property
    def usage_point(self):
        return self._configuration.usage_point

    def _execute(self):
        root = config.INGEST_ENDPOINT

        # SDG&E Green Button does not offer more than 2 years of historical data.
        # Truncate so that we don't make trivial requests.
        start = max(self.start_date, date(2017, 1, 1))
        end = min(self.end_date, date.today())

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
            payload = SearchSpecification(
                usage_point=self.usage_point,
                start=start_dt - timedelta(days=2),
                end=end_dt + timedelta(days=2),
            )
            headers = {"Content-Type": "application/json"}
            log.debug("Request JSON: %s", payload.to_json())
            response = requests.post(
                root + "/sdge/search", json=payload.to_json(), headers=headers
            )
            log.debug("Response JSON: %s" % response.json())
            service_updates.append(ServiceUpdate(response.json()))

        load_curves = [lc for su in service_updates for lc in su.load_curves]
        interval = determine_interval(load_curves)
        timeline = Timeline(start, end, interval=interval)
        bills = set()
        for su in service_updates:
            for curve in su.load_curves:
                multiplier = determine_demand_multiplier(curve.unit, interval)
                for v in curve.values:
                    log.debug(
                        "Inserting interval data: %s - %s",
                        _adjust_timezone(v.start),
                        multiplier * v.value,
                    )
                    timeline.insert(_adjust_timezone(v.start), multiplier * v.value)

            for b in su.bills:
                if b.total is None:
                    log.info("Skipping bill with null total: %s" % b)
                    continue

                if b.use is None:
                    log.info("Skipping bill with null use: %s" % b)
                    continue

                use = b.use / 1000 if b.unit == "Wh" else b.use
                bills.add(
                    BillingDatum(
                        start=_adjust_timezone(b.start).date(),
                        end=_adjust_timezone(b.end).date(),
                        statement=_adjust_timezone(b.end).date(),
                        cost=b.total,
                        used=use,
                        peak=None,  # Peak, line items, and attachments aren't available.
                        items=None,
                        attachments=None,
                        utility_code=None,
                    )
                )

        final_bills = adjust_bill_dates(list(bills))
        return Results(readings=timeline.serialize(), bills=final_bills)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: SnapmeterMeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    configuration = SdgeGreenButtonAccountConfiguration(
        usage_point=datasource.meta.get("usagePoint")
    )

    return run_datafeed(
        SdgeGreenButtonScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
