import logging

from datetime import timedelta, datetime
from typing import List, Optional

from datafeeds import db
from datafeeds.common import DateRange
from datafeeds.common.typing import (
    BillingDatum,
    BillingData,
    show_bill_summary,
    assert_is_without_overlaps,
    NoFutureBillsError,
)
from datafeeds.common.support import Configuration
from datafeeds.models.bill import GENERATION_ONLY, TND_ONLY
from datafeeds.models.meter import Meter
from datafeeds.models.bill import PartialBill

log = logging.getLogger(__name__)


def adjust_billing_datum_type(bill: BillingDatum):
    """
    Handles when datetimes are passed into BillingDatum.

    Replaces start/end on the BillingDatum object with dates
    instead of datetimes, if applicable
    """
    bill_start = bill.start
    bill_end = bill.end

    if type(bill_start) == datetime:
        bill_start = bill_start.date()  # type: ignore

    if type(bill_end) == datetime:
        bill_end = bill_end.date()  # type: ignore

    return bill._replace(start=bill_start, end=bill_end)


class PartialBillProcessor:
    def __init__(
        self, meter: Meter, scraper_config: Configuration, billing_data: BillingData
    ):
        """
        Partial bills are "intermediate" bills that are assumed to contain only a subset of the
        total charges - just T&D charges, or just generation charges, for example.

        We compare incoming partial bills to existing partial bills in the db.  If data has changed,
        or cycles overlap, existing partial bills are marked as superseded instead of modifying the
        existing partial bill. New partial bills are then created.

        Partial bills are written directly to the partial bills table.

        :param meter: Meter object
        :param scraper_config: Original scraper Configuration
        :param billing_data: Pending partial bills.  Because partial bills/bill objects share core fields,
        this is a BillingData type.
        """
        self.meter = meter
        self.configuration = scraper_config
        self.staged_partial: List[PartialBill] = []
        self.superseded: List[PartialBill] = []

        # Adjusting billing_data for type consistency on date
        for i, bd in enumerate(billing_data):
            billing_data[i] = adjust_billing_datum_type(bd)
        self.billing_data = billing_data

    @property
    def partial_bills_type(self):
        """
        Infers if the incoming partial bills are "tnd-only" bills or "generation-bills" by matching on the service_id that
        was passed to the scraper.  If the gen_service_id was passed in, we assume this is a generation partial bill.
        It is possible that the gen_service_id is stored on the Configuration.service_id field, so be flexible.
        """
        said = getattr(self.configuration, "service_id", None) or getattr(
            self.configuration, "gen_service_id", None
        )

        if said == self.meter.utility_service.gen_service_id:
            return GENERATION_ONLY
        return TND_ONLY

    @property
    def haves(self) -> List[PartialBill]:
        """
        Returns existing partial bills attached to the same service that
        have the same provider type (generation-only or T&D only)
        as the newly scraped pending partial bills.

        Only returns partial bills that haven't been superseded by a later partial bill.
        """
        return (
            db.session.query(PartialBill)
            .filter(PartialBill.service == self.meter.service)
            .filter(PartialBill.provider_type == self.partial_bills_type)
            .filter(PartialBill.superseded_by.is_(None))
            .order_by(PartialBill.initial.asc())
        )

    @staticmethod
    def _bad_override_detected(existing: PartialBill, pending: BillingDatum) -> bool:
        """
        Whether an incoming partial bill with zero usage was detected.  This partial bill
        will be ignored if we're trying to override an existing partial bill with non-zero usage.
        """
        bad_override = (
            pending.cost != 0.0 and pending.used == 0 and existing.used != 0.0
        )
        if bad_override:
            show_bill_summary([pending], "Partial bill ignored; zero usage detected.")
        return bad_override

    @staticmethod
    def _existing_is_manual(existing: PartialBill, pending: BillingDatum) -> bool:
        """
        Returns true if the existing PartialBill is manual.  The purpose of this method
        is to largely consolidate logging of manual issues
        """
        if existing.manual:
            show_bill_summary(
                [pending], "Partial bill ignored; existing partial bill is manual."
            )
        return existing.manual

    @staticmethod
    def _show_partial_bill_summary(partial_bills: List[PartialBill], title=None):
        """Save our results to the log for easy reference."""

        if title:
            log.info("=" * 80)
            log.info(title)
            log.info("=" * 80)

        fields = ("Start", "End", "Cost", "Use", "Peak", "Has PDF", "Type")
        fmt = "%-10s  %-10s  %-10s  %-10s  %-10s %-10s %-10s"
        log.info(fmt % fields)
        for pb in partial_bills:
            entries = [
                str(pb.initial),
                str(pb.closing),
                str(pb.cost),
                str(pb.used),
                str(pb.peak),
                str(pb.attachments != []),
                str(pb.provider_type),
            ]
            log.info(fmt % tuple(entries))
        log.info("=" * 80)

    def _find_staged(self, pending_partial: BillingDatum) -> Optional[PartialBill]:
        """
        Returns the partial bill that was already created from this partial billing
        datum in this session, if applicable.
        """
        for staged in self.staged_partial:
            if staged.matches(pending_partial):
                return staged
        return None

    def _supersede(self, existing_partial: PartialBill, pending_partial: BillingDatum):
        """
        Supersedes the existing partial bill with a new partial bill.

        Looks to see if the superseding partial bill was already created during this
        session, otherwise creates a new partial bill.

        Marks the existing bill with "superseded_by".
        """
        superseding = self._find_staged(pending_partial)

        if not superseding:
            # Create a new partial bill, if one hasn't been created already
            superseding = PartialBill.generate(
                self.meter.service, self.partial_bills_type, pending_partial
            )
            # Added for logging purposes
            self.staged_partial.append(superseding)

        existing_partial.supersede(superseding)
        # Added for logging purposes
        self.superseded.append(existing_partial)

    def _snap_first_start_date(self):
        """
        Adjusts the start date of a new partial bill, such that its start date
        is equal to the end date of an existing partial bill. This helps us
        "snap" the start date into the existing bill timeline.

        One day is added to the start date, if applicable.
        """
        if self.billing_data:
            new_bill = self.billing_data[0]
            for existing_bill in self.haves:
                if new_bill.start == existing_bill.closing:
                    self.billing_data[0] = new_bill._replace(
                        start=new_bill.start + timedelta(days=1)
                    )
                    log.info(
                        "Snapped the start date of the first new bill to {}".format(
                            self.billing_data[0].start
                        )
                    )
                    break

    def process_partial_bills(self):
        """ Primary method.

        Goes through billing_data and uploads new partial bills directly to the partial bills table.
        If a new partial bill differs from an existing partial bill,
        a new partial bill is created, rather than overwriting the old one.
        """
        # Run initial validation of all the partial bills.  Failure will cause a premature exit.
        PartialBillValidator(self.billing_data).run_prevalidation()

        # Snap the start date of the first new bill, if applicable
        self._snap_first_start_date()

        for pending_partial in self.billing_data:
            found = False
            for existing_partial in self.haves:
                existing_cycle = DateRange(
                    existing_partial.initial, existing_partial.closing
                )
                pending_cycle = DateRange(pending_partial.start, pending_partial.end)

                if existing_cycle == pending_cycle:  # cycles match exactly
                    if (
                        existing_partial.differs(pending_partial)
                        and not self._bad_override_detected(
                            existing_partial, pending_partial
                        )
                        and not self._existing_is_manual(
                            existing_partial, pending_partial
                        )
                    ):
                        # Mark the old partial bill as superseded
                        # and add a new partial bill
                        self._supersede(existing_partial, pending_partial)
                    found = True
                    break
                elif existing_cycle.intersects(
                    pending_cycle
                ):  # cycle doesn't match exactly, but intersects.
                    if not self._existing_is_manual(existing_partial, pending_partial):
                        # We create a new partial bill and supersede the old one
                        self._supersede(existing_partial, pending_partial)
                    found = True

            if not found:
                # Pending partial bill doesn't already exist, so we stage a new one
                pb = PartialBill.generate(
                    self.meter.service, self.partial_bills_type, pending_partial
                )
                self.staged_partial.append(pb)

    def log_summary(self):
        """Logs the partial bills scraped.

        Additionally, logs the partial bills written to the db, as well as the partial bills superseded,
        for debugging.
        """
        # Logs summary of all partial bills scraped, for debugging purposes
        show_bill_summary(self.billing_data, "Summary of all Scraped Partial Bills")

        # Logs summary of just the new partial bills that were written to the db.
        if self.staged_partial:
            self._show_partial_bill_summary(
                self.staged_partial, "New Partial Bills Written to DB"
            )
        else:
            log.info("No new partial bills written to the db.")

        # Logs summary of partial bills that were superseded during this scraper run.
        if self.superseded:
            self._show_partial_bill_summary(self.superseded, "Superseded Partial Bills")
        else:
            log.info("No partial bills superseded.")


class PartialBillValidator:
    def __init__(self, billing_data: BillingData):
        """
        Initial partial bill validator that checks that some billing requirements
        have been met, similar to the validation that was running in stasis
        transactions in platform.
        """
        self.billing_data = billing_data

    def run_prevalidation(self):
        """
        Runs initial validation on incoming partial bills.  If any of these validation
        checks fail, we will exit early with an error.

        Add additional requirements below as needed.
        """
        self._bill_import_overlap_requirement()
        self._no_future_bills_requirement()

    def _bill_import_overlap_requirement(self):
        """
        Requirement: incoming partial bills cannot overlap
        """
        assert_is_without_overlaps(self.billing_data)

    def _no_future_bills_requirement(self):
        """
        Requirement: incoming partial bills cannot end in the future.
        """
        today = datetime.today().date()
        for pb in self.billing_data:
            if pb.end > today:
                show_bill_summary([pb], "Future Bill Detected.")
                raise NoFutureBillsError()
