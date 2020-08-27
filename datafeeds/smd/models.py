""" models.py

This is a lightly-revised copy of webapps ORM for managing provisioning records.

Datafeeds needs access to this ORM so that it can look up how to process a particular workflow and anotate
the outcome of the scraper run. In general, the ORM should not be used to construct new records (other than
provisioning events) since webapps handles this.
"""


import copy
from collections import namedtuple
from enum import Enum, unique
from datetime import datetime
import logging
from typing import Optional, List, Set, Tuple, Generator

import sqlalchemy as sa
from sqlalchemy import orm

# pylint: disable=no-name-in-module
from jsonobject import (
    JsonObject,
    StringProperty,
    ListProperty,
    BooleanProperty,
    IntegerProperty,
    ObjectProperty,
)

# pylint: disable=no-name-in-module
from jsonobject.exceptions import BadValueError

from datafeeds import db
from datafeeds.models import SnapmeterAccount
from datafeeds.orm import ModelMixin, Base
from datafeeds.smd import encryption

log = logging.getLogger(__name__)


class InvalidStateTransition(Exception):
    pass


class MetadataIntegrityException(Exception):
    pass


class InvalidMetadataException(Exception):
    pass


# These classes cover converting between JSON records (saved to the database) and Python objects.
# The benefit of using these classes is that we get schema checking for free as a function of using the
# jsonobject library. The "schema" we expect is dictated by the Property types in each class definition.


class MeterSummary(JsonObject):
    address = StringProperty()
    service_id = StringProperty()
    service_uuid = (
        StringProperty()
    )  # This is the usage-point identifier in SMD green button.
    service_type = StringProperty()


class AccountSummary(JsonObject):
    name = StringProperty()
    account_number = StringProperty()
    account_uuid = StringProperty()
    meters = ListProperty(MeterSummary)


class AuthorizationSummary(JsonObject):
    found_authorized = BooleanProperty()
    subscription_id = StringProperty()
    accounts = ListProperty(AccountSummary)
    error_data = StringProperty()


class StagingAddress(JsonObject):
    street1 = StringProperty()
    street2 = StringProperty()
    city = StringProperty()
    state = StringProperty()
    zip = StringProperty()

    def formatted(self):
        """Return the address JSON with street and city in a standardized format."""

        return StagingAddress(
            street1=self.street1.title() if self.street1 is not None else None,
            street2=self.street2.title() if self.street2 is not None else None,
            city=self.city.title() if self.city is not None else None,
            state=self.state,
            zip=self.zip,
        )


class StagingMeterData(JsonObject):
    usage_point = (
        IntegerProperty()
    )  # DEPRECATED : OID of usage point objects in green button staging.
    usage_point_id = StringProperty()  # Use this instead.
    duration = IntegerProperty()
    address = ObjectProperty(StagingAddress)
    account_number = StringProperty()
    service_id = StringProperty()
    utility = StringProperty()
    tariff = StringProperty()
    commodity = StringProperty()

    def is_complete(self):
        record = self.to_json()
        record.pop("usage_point")
        return all(x is not None for x in record.values())

    def __eq__(self, other):
        if not isinstance(other, StagingMeterData):
            return False

        return self.to_json() == other.to_json()


class StagingSummary(JsonObject):
    meters = ListProperty(StagingMeterData)


class CreationSummary(JsonObject):
    meters = ListProperty(int)


#
# The State enumeration tracks where we are in the process of provisioning with one credential.
#
# Failed:
#   The provisioning processor should not try to do anything more with this task. Human intervention is needed.
#
# Begin:
#   This is the default state for a newly created workflow. Nothing has happened yet.
#
# Authorized:
#   We attempted to log into pge.com and authorize the SAIDs available with these credentials. No error flag
#   indicates the authorization succeeded.
#
# Verified:
#   In this step we log back into the SMD authorization site, determine if the utility has finished processing
#   the authorization, and obtain the retail customer identifier if the authorization is complete.
#
# Data Staged:
#   Now that we know what SAIDs to look for, we looked in staging for the relevant meters.
#   If we see the relevant staging objects, we can proceed.
#
# Objects Created:
#   This state records that generated meters, buildings, utility services, and product enrollments from the objects
#   we found in staging.
#
# Data Imported:
#   In this step, we triggered a job in tasks to migrate staging data into our system.
#
# Analytics Ran:
#   In this step, we triggered analytics for newly created meters.
#
# Complete:
#   The provisioning process is over. The processor doesn't need to do any more work.
#
# Refresh Requested:
#   A user has requested that we refresh the authorization (de-authorize and re-authorize) in order to capture
#   new accounts/services that PG&E may have added.
#
# Data Sources Revised:
#   We successfully removed the Meter Data Source and Account Data Source records that will be rendered useless
#   once we de-authorize. Cancel the SMD subscription via API call.
#
# Deprovisioning Requested:
#   A user has requested that we delete this workflow, and the account associated with it.
#
# Account Deactivated:
#   This indicates that we have moved the snapmeter account from the domain ce-portal to ce-deprovisioned,
#   disabled all of the product enrollments for the account, and cancelled all data sources and subscriptions for SMD
#   meters in the workflow.
#
# Deprovisioning Complete:
#   At this point, the account should no longer be visible in the CE portal, and its meters should not show up
#   in CE-related reports (bill audit, rate analysis, etc.).
#
# Note: We may move from the authorization removed state back to the authorized state
# (and cycle through the states after that) in order to provision newly discovered meters (i.e. the directed graph
# of state transitions has a loop in it).
#

# Lint complains about inheriting from Enum here, but we definitely need this.
# pylint: disable=too-many-ancestors


class AutoNumber(Enum):
    def __new__(cls):
        value = len(cls.__members__) + 1
        obj = object.__new__(cls)
        obj._value_ = value
        return obj


@unique
class State(AutoNumber):
    # Note: The ordering of the states below matters.
    failed = ()
    begin = ()
    authorized = ()
    verified = ()
    data_staged = ()
    objects_created = ()
    data_imported = ()
    analytics_ran = ()
    complete = ()
    refresh_requested = ()
    data_sources_revised = ()
    deprovisioning_requested = ()
    account_deactivated = ()
    deprovisioning_complete = ()


class Credential(ModelMixin, Base):
    """A credential records a username/password pair that can be used to establish SMD access at pge.com."""

    __tablename__ = "snapmeter_provisioning_credential"

    oid = sa.Column(sa.BigInteger, primary_key=True)

    workflow = orm.relationship("Workflow", back_populates="credential")

    # These columns should be considered private. We shouldn"t store credentials unencrypted,
    # so we enforce that requirement by accessing these fields through the methods below.
    _password_bytes = sa.Column("password_bytes", sa.LargeBinary)
    _username_bytes = sa.Column("username_bytes", sa.LargeBinary)

    def __init__(self, username, password):
        self.encrypt_user(username)
        self.encrypt_password(password)

    @property
    def password(self):
        if self._password_bytes:
            return encryption.aes_decrypt(self._password_bytes)

        return None

    def encrypt_password(self, pwd):
        self._password_bytes = encryption.aes_encrypt(pwd) if pwd else None

    @property
    def username(self):
        if self._username_bytes:
            return encryption.aes_decrypt(self._username_bytes)

        return None

    def encrypt_user(self, user):
        self._username_bytes = encryption.aes_encrypt(user) if user else None


# Other modules will need to read the event log in order to decide what action to take next.
# However, we do not want to give out references to this log, because it's mutable (and in this case,
# mutating the log could yield unexpected behavior). EventRecords allow us to copy the important event
# data without exposing unnecessary details about the ORM.


class EventRecord(
    namedtuple("EventRecord", ["occurred", "error", "state", "message", "meta"])
):
    def __str__(self):
        template = "<EventRecord state: %s, error: %s, occurred: %s>"
        return template % (str(self.state.name), self.error, self.occurred)


class Event(ModelMixin, Base):
    """Events capture each transition in the provisioning lifecycle.
    They are attached to the workflow in a chronologically ordered, immutable log.

    Note: With few exceptions, most workflows will have a small (<20) steps.
    """

    __tablename__ = "snapmeter_provisioning_event"

    # Some states are expected to be accompanied by certain metadata. These are captured in the metadata map.
    _metadata_map = {
        State.authorized: AuthorizationSummary,
        State.verified: AuthorizationSummary,
        State.data_staged: StagingSummary,
        State.objects_created: CreationSummary,
    }

    oid = sa.Column(sa.BigInteger, nullable=False, primary_key=True)

    workflow_id = sa.Column(
        "workflow",
        sa.BigInteger,
        sa.ForeignKey("snapmeter_provisioning_workflow.oid"),
        nullable=False,
    )
    workflow = orm.relationship("Workflow", back_populates="_events")

    # This field occurs when the state transition was completed.
    occurred = sa.Column(sa.DateTime, nullable=False, default=datetime.utcnow)

    # This field records whether there was an un-recoverable error running the current step.
    error = sa.Column(sa.Boolean, nullable=False, default=False)

    # This enum column indicates the step in the provisioning process that this event references.
    state = sa.Column(sa.Enum(State), nullable=False, default="begin")

    # The message field records what happened while the work was in this state. (e.g. "Created 22 meters.")
    message = sa.Column(sa.Unicode, nullable=False)

    # This field can be used to store identifiers that will be used in later steps.
    meta = sa.Column(sa.JSON)

    def __init__(
        self,
        state: State,
        message: str,
        error: bool = False,
        metadata: Optional[JsonObject] = None,
        occurred: datetime = None,
    ):
        self.state = state
        self.message = message
        self.error = error
        self.occurred = occurred

        expected_type = self._metadata_map.get(self.state)

        if expected_type is None:
            if metadata is not None:
                msg = "An event for state %s cannot have metadata."
                raise InvalidMetadataException(msg % state.name)
        else:
            if error and metadata is None:
                pass  # Error events are allowed to have no metadata, regardless of state.
            elif isinstance(metadata, expected_type):
                self.meta = metadata.to_json()
            else:
                msg = "An event for state %s requires metadata of type %s. Found %s."
                raise InvalidMetadataException(
                    msg % (state.name, expected_type, type(metadata))
                )

    def __str__(self):
        template = "<Event oid: %s, state: %s, error: %s, occurred: %s>"
        return template % (self.oid, str(self.state.name), self.error, self.occurred)

    def __repr__(self):
        return str(self)

    def to_json(self):
        """Return a JSON representation of the workflow suitable for consumption by Ember."""
        return dict(
            oid=self.oid,
            occurred=str(self.occurred),
            error=self.error,
            state=self.state.name,
            message=self.message,
        )

    def to_record(self):
        meta_type = self._metadata_map.get(self.state)
        if meta_type is None:
            if self.meta is not None:
                msg = "Found non-null metadata for event (oid: %s) in state %s."
                log.error(msg, self.oid, self.state.name)
                raise MetadataIntegrityException(msg % (self.oid, self.state.name))
            meta = None
        else:
            # Decode JSON from the database to a JsonObject
            try:
                meta = meta_type(copy.deepcopy(self.meta))
            except BadValueError:
                log.exception("Failed to deserialize event (oid: %s).", self.oid)

        return EventRecord(
            occurred=self.occurred,
            error=self.error,
            state=self.state,
            message=self.message,
            meta=meta,
        )


class Workflow(ModelMixin, Base):
    """A Workflow tracks the progress the system has made setting up meters authorized by a single credential."""

    __tablename__ = "snapmeter_provisioning_workflow"

    oid = sa.Column(sa.BigInteger, nullable=False, primary_key=True)

    # Computing whether the workflow is complete using database queries alone is rather difficult/expensive.
    # Instead we cache whether the workflow is complete here, and update whenever we add a new event.
    complete = sa.Column(sa.Boolean)

    parent_id = sa.Column(
        "parent",
        sa.BigInteger,
        sa.ForeignKey("snapmeter_provisioning.oid"),
        nullable=False,
    )
    parent = orm.relationship("Provisioning", back_populates="workflows")

    _credential = sa.Column(
        "credential",
        sa.ForeignKey("snapmeter_provisioning_credential.oid"),
        nullable=False,
    )
    credential = orm.relationship("Credential", back_populates="workflow")

    # A log recording events leading up to full provisioning. Treat this member as private.
    _events = orm.relationship("Event", cascade="all,delete")

    def __init__(self, parent, credential):
        self.parent = parent
        self.credential = credential

        # Every workflow starts with a "begin" event, so we need to create one here.
        e = Event(
            state=State.begin,
            message="The system is ready to begin provisioning with these credentials.",
            occurred=datetime.utcnow(),
        )
        self._events.append(e)
        self.complete = False

    def to_json(self):
        """Return a JSON representation of the workflow suitable for consumption by Ember."""
        return dict(
            oid=self.oid,
            credential=dict(
                username=self.credential.username, password=self.credential.password
            ),
            events=[e.to_json() for e in self._chronological_events()],
            complete=self.is_complete(),
            deprovisioned=self.is_deprovisioned(),
        )

    def is_complete(self) -> bool:
        if self.complete is None:
            self.complete = self._latest_event().state in {
                State.complete,
                State.deprovisioning_complete,
            }

        return self.complete

    def is_deprovisioned(self) -> bool:
        return self._latest_event().state == State.deprovisioning_complete

    def has_failed(self):
        return self._latest_event().state == State.failed

    def __str__(self):
        latest = self._latest_event()
        template = "<Workflow oid: %s, state: %s, last update: %s>"
        return template % (self.oid, latest.state.name, latest.occurred)

    def __repr__(self):
        return str(self)

    @classmethod
    def _is_valid_state_transition(cls, current: State, proposed: State) -> bool:
        if current == State.data_sources_revised:
            return (
                proposed == State.authorized or proposed == State.data_sources_revised
            )

        if current == State.complete:
            return (
                proposed == State.refresh_requested
                or proposed == State.deprovisioning_requested
            )

        return (
            current == proposed
            or current.value + 1 == proposed.value
            or proposed == State.failed
        )

    def create_event(
        self,
        state: State,
        message: str,
        error: bool = False,
        metadata: JsonObject = None,
    ):
        """Introduce a new event at the end of the workflow."""
        latest_state = self._latest_event().state
        if not self._is_valid_state_transition(latest_state, state):
            msg = "Cannot transition from %s to %s directly."
            raise InvalidStateTransition(msg % (latest_state.name, state.name))

        e = Event(
            state=state,
            message=message,
            error=error,
            metadata=metadata,
            occurred=datetime.utcnow(),
        )
        self._events.append(e)
        self.complete = e.state in {State.complete, State.deprovisioning_complete}

    def _chronological_events(self) -> List[Event]:
        return sorted(self._events, key=lambda ev: ev.occurred)

    def _latest_event(self) -> Event:
        return self._chronological_events()[-1]

    def events(self) -> Generator[EventRecord, None, None]:
        """Produce a generator that yields a read-only copy of the event log."""
        for e in self._chronological_events():
            yield e.to_record()

    def latest(self) -> EventRecord:
        return list(self.events())[-1]

    def latest_of(self, state: State) -> Optional[EventRecord]:
        for e in reversed(list(self.events())):
            if e.state == state:
                return e

        return None

    @property
    def subscription_id(self) -> Optional[str]:
        er = self.latest_of(State.verified)
        if er:
            return er.meta.subscription_id

        return None


class IncompleteProvisioningException(Exception):
    pass


class Provisioning(ModelMixin, Base):
    """A snapmeter account provisioning task consists of one or more workflows, one for each credential the user
     supplied. The meters discovered in each workflow will get added to the same final Snapmeter account."""

    # Constants for interfacing with Elasticsearch
    es_index = "snapmeter-provisioning"
    doc_type = "provisioning"

    __tablename__ = "snapmeter_provisioning"

    oid = sa.Column(sa.BigInteger, nullable=False, primary_key=True)

    # If a provisioning run has not made progress after a certain amount of time, the active field allows us
    # to mark the process as inactive, even if it isn"t complete. Conversely, code that managing updates for
    # provisioning objects can mark an object as inactive once all work on it is complete.
    active = sa.Column(sa.Boolean, default=True, nullable=False)

    # An optional parent account and salesforce ID columns.
    # It"s not clear if this is used in practice, but may be needed
    # to integrate with SalesForce properly.
    parent_account_name = sa.Column(sa.Unicode)
    salesforce_contract_id = sa.Column(sa.Unicode)

    _account = sa.Column(
        "account", sa.ForeignKey("snapmeter_account.oid"), nullable=False
    )
    account = orm.relationship("SnapmeterAccount")

    workflows = orm.relationship("Workflow", back_populates="parent")

    def __init__(
        self,
        account: SnapmeterAccount,
        credentials: Optional[List[Tuple[str, str]]] = None,
        parent_account_name: Optional[str] = None,
        salesforce_contract_id: Optional[str] = None,
    ):
        """Create a new provisioning task."""
        self._account = account.oid
        self.parent_account_name = parent_account_name
        self.salesforce_contract_id = salesforce_contract_id

        if credentials is None:
            credentials = []

        for cred in credentials:
            self.add_credential(*cred)

    def add_credential(self, username: str, password: str) -> Optional[Workflow]:
        for w in self.workflows:
            if w.credential.username == username and w.credential.password == password:
                return None  # This credential is already captured.

        cred = Credential(username, password)
        w = Workflow(self, cred)
        self.index()
        return w

    def is_complete(self) -> bool:
        return all(w.is_complete() for w in self.workflows)

    def is_deprovisioned(self) -> bool:
        return all(w.is_deprovisioned() for w in self.workflows)

    def deprovision(self) -> None:
        if not self.is_complete():
            raise IncompleteProvisioningException(
                "Not all workflows in this provisioning are complete."
            )

        for w in self.workflows:
            w.create_event(
                State.deprovisioning_requested,
                "We have initiated the process of deprovisioning this account.",
            )

    def to_json(self, limit: Optional[Set[int]] = None) -> dict:
        """Return a JSON representation suitable for consumption by Ember."""

        if limit:
            workflows = [w for w in self.workflows if w.oid in limit]
        else:
            workflows = self.workflows

        return dict(
            oid=self.oid,
            account_oid=self._account,
            account_name=self.account.name,
            parent_account_name=self.parent_account_name,
            salesforce_contract_id=self.salesforce_contract_id,
            account_hex_id=self.account.hex_id,
            active=self.active,
            complete=self.is_complete(),  # Convenience flag for the frontend, summarizes workflows.
            deprovisioned=self.is_deprovisioned(),
            workflows=[w.to_json() for w in workflows],
        )

    def __str__(self) -> str:
        complete = [w for w in self.workflows if w.is_complete()]
        template = "<Provisioning oid: %s, account: %s, workflows: %s of %s complete>"
        return template % (self.oid, self._account, len(complete), len(self.workflows))

    def __repr__(self) -> str:
        return str(self)

    @classmethod
    def by_account(cls, account: SnapmeterAccount):
        """Locate the provisioning associated with the input snapmeter account, or create one if none exists."""
        provisioning = (
            db.session.query(Provisioning)
            .filter(Provisioning._account == account.oid)
            .first()
        )

        if not provisioning:
            provisioning = Provisioning(account)
            db.session.add(provisioning)
            db.session.flush()
            db.session.refresh(provisioning)

        return provisioning
