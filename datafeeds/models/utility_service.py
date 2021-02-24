"""Utility Service

This module covers tables managed by webapps/platform that describe utility services.
Except for unit tests, analytics should treat these tables as Read Only.
"""
from datetime import datetime
import logging
from typing import Optional, Dict, Any, List
import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.orm import relationship, Session
from sqlalchemy.orm.attributes import get_history

from datafeeds import db
from datafeeds.orm import ModelMixin, Base


log = logging.getLogger(__name__)

UTILITY_BUNDLED = "utility-bundled"
TND_ONLY = "tnd-only"
GENERATION_ONLY = "generation-only"

PROVIDER_TYPES = [UTILITY_BUNDLED, TND_ONLY]


class UtilityService(ModelMixin, Base):
    __tablename__ = "utility_service"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    account = sa.Column(sa.BigInteger)  # unused
    # active = sa.Column(sa.Boolean, nullable=False)  # unused
    # group = sa.Column(sa.Unicode)  # unused
    # options = sa.Column(JSON)  # unused
    service_id = sa.Column(sa.Unicode)
    tariff = sa.Column(sa.Unicode)
    utility = sa.Column(sa.Unicode)
    utility_account_id = sa.Column(sa.Unicode)
    provider_type = sa.Column(sa.Enum(*PROVIDER_TYPES), default=PROVIDER_TYPES[0])

    # Generation configuration information.  If a meter has
    # a third party responsible for generating power,
    # that service information is stored in these fields.
    gen_service_id = sa.Column(sa.Unicode)
    gen_tariff = sa.Column(sa.Unicode)
    gen_utility = sa.Column(sa.Unicode)
    gen_utility_account_id = sa.Column(sa.Unicode)
    gen_options = sa.Column(sa.JSON)

    meter = relationship("Meter", back_populates="utility_service")

    # Don't use this constructor outside of tests.
    def __init__(
        self,
        service_id: str,
        account_id: Optional[str] = None,
        gen_service_id: Optional[str] = None,
    ):
        self.service_id = service_id
        self.utility_account_id = account_id
        self.gen_service_id = gen_service_id

    @classmethod
    def copy_from(cls, other: "UtilityService"):
        """Create a non-persisted copy of a utility_service record."""
        service_copy = UtilityService(
            service_id=other.service_id,
            account_id=other.utility_account_id,
            gen_service_id=other.gen_service_id,
        )
        for col in cls.__mapper__.columns:  # type: ignore
            if col.name in ["oid"]:
                continue
            setattr(service_copy, col.name, getattr(other, col.name))
        return service_copy

    def set_tariff_from_utility_code(
        self, utility_tariff_code: str, provider_type: str
    ):
        log.info(
            "map utility tariff %s service, utility=%s tariff=%s",
            provider_type,
            self.utility,
            utility_tariff_code,
        )
        # TODO: no-op for now until we have the mappings

    def updates(self) -> Dict[str, Any]:
        if not db.session.is_modified(self):
            return {}
        changes: Dict[str, Any] = {}
        messages: List[str] = []
        # track changes to these fields
        fields = [
            "service_id",
            "tariff",
            "utility_account_id",
            "gen_service_id",
            "gen_tariff",
            "gen_utility",
            "gen_utility_account_id",
            "provider_type",
        ]
        for field in fields:
            history = get_history(self, field)
            added = history.added[0] if history.added else None
            deleted = history.deleted[0] if history.deleted else None
            changes[field] = added
            changes["%s_prev" % field] = deleted
            # clear: History(added=[None], unchanged=(), deleted=['value'])
            if deleted and not added:
                messages.append("%s: cleared %s." % (field, added))
            # add: History(added=['123'], unchanged=(), deleted=[None])
            if added and not deleted:
                messages.append("%s: set %s (was unset)." % (field, added))
            # update: History(added=['B-19-S'], unchanged=(), deleted=['E-19-S'])
            if added and deleted:
                messages.append("%s: updated %s (was %s)." % (field, added, deleted))
        return {
            "message": "\n".join(messages),
            "fields": changes,
        }


class UtilityServiceSnapshot(ModelMixin, Base):
    """utility_service_snapshot table
    tracks updates to utility_services - each record is intended to be a "snapshot" of the
    utility_service at that point in time.
    """

    __tablename__ = "utility_service_snapshot"

    INITIAL_SNAPSHOT_DATETIME = datetime(1970, 1, 1)

    oid = sa.Column(sa.BigInteger, primary_key=True)
    service = sa.Column(
        sa.BigInteger, sa.ForeignKey("utility_service.oid"), nullable=False
    )
    service_obj = relationship("UtilityService")
    # Tracking updates for the "main" service - whether bundled or T&D
    # Type of the "main" provider"
    provider_type = sa.Column(sa.Enum(*PROVIDER_TYPES))
    service_id = sa.Column(sa.Unicode)
    utility_account_id = sa.Column(sa.Unicode)
    tariff = sa.Column(sa.Unicode)
    utility = sa.Column(sa.Unicode)
    # Tracking updates for the "generation" service, if applicable
    gen_service_id = sa.Column(sa.Unicode)
    gen_utility_account_id = sa.Column(sa.Unicode)
    gen_tariff = sa.Column(sa.Unicode)
    gen_utility = sa.Column(sa.Unicode)
    # When the snapshot was created in our system
    system_created = sa.Column(sa.DateTime, nullable=False)
    # When the snapshot was updated in our system
    system_modified = sa.Column(sa.DateTime, nullable=False)
    # The main date we care about - when the service agreement
    # was modified on the utility's side.
    service_modified = sa.Column(sa.DateTime, nullable=False)

    @classmethod
    def generate(
        cls,
        service: UtilityService,
        service_modified: Optional[datetime] = None,
        system_created: Optional[datetime] = None,
        system_modified: Optional[datetime] = None,
    ):
        """When services are updated, generate a snapshot"""
        if not service_modified:
            service_modified = datetime.now()

        if not system_created:
            system_created = datetime.now()

        if not system_modified:
            system_modified = datetime.now()

        # if one already exists, update it
        snapshot = None
        if service and service.oid:
            # query by the unique key
            snapshot = (
                db.session.query(UtilityServiceSnapshot)
                .filter(
                    UtilityServiceSnapshot.service == service.oid,
                    UtilityServiceSnapshot.service_modified == service_modified,
                )
                .first()
            )
        if not snapshot:
            snapshot = UtilityServiceSnapshot(
                service_obj=service,  # service.oid may not exist yet
                system_created=system_created,
                service_modified=service_modified,
            )
        snapshot.update(
            **{
                "provider_type": service.provider_type,
                "service_id": service.service_id,
                "utility_account_id": service.utility_account_id,
                "tariff": service.tariff,
                "utility": service.utility,
                "gen_service_id": service.gen_service_id,
                "gen_utility_account_id": service.gen_utility_account_id,
                "gen_tariff": service.gen_tariff,
                "gen_utility": service.gen_utility,
                "system_modified": system_modified,
            }
        )
        db.session.add(snapshot)
        return snapshot

    def update(self, **kwargs) -> bool:
        """For updating an incorrect, existing snapshot with the correct information.
        For example, a snapshot was created when we noticed a tariff had changed, but the tariff actually changed
        a year prior.  We can update the service_modified of the existing snapshot to be that earlier date.
        Returns whether the snapshot was updated.
        """
        updated = False
        for field in kwargs:
            if hasattr(self, field) and getattr(self, field) != kwargs[field]:
                setattr(self, field, kwargs[field])
                updated = True

        if updated:
            self.system_modified = datetime.now()
            db.session.add(self)

        return updated


@event.listens_for(Session, "before_flush")
def receive_before_flush(session, flush_context, instances):
    """Create UtilityServiceSnapshot records for new or updated UtilityService objects.

    https://docs.sqlalchemy.org/en/13/orm/session_events.html#before-flush
    Use SessionEvents.before_flush() in order to operate upon objects to validate their state as
    well as to compose additional objects and references before they are persisted. Within this
    event, it is safe to manipulate the Session’s state, that is, new objects can be attached to
    it, objects can be deleted, and individual attributes on objects can be changed freely, and
    these changes will be pulled into the flush process when the event hook completes.

    Use this instead of after_insert or after_updated, even though it's more trouble:
    https://docs.sqlalchemy.org/en/13/orm/session_events.html#mapper-level-events
    However, the flush plan which represents the full list of every single INSERT, UPDATE, DELETE
    statement to be emitted has already been decided when these events are called, and no changes
    may be made at this stage. Therefore the only changes that are even possible to the given
    objects are upon attributes local to the object’s row. Any other change to the object or other
    objects will impact the state of the Session, which will fail to function properly.

    Operations that are not supported within these mapper-level persistence events include:
    Session.add()
    """
    for instance in session.dirty:
        if not session.is_modified(
            instance, include_collections=False
        ) or not isinstance(instance, UtilityService):
            continue
        # If a utility service has been modified, create a snapshot.
        log.debug("generating utility service snapshot; %s updated", instance.oid)
        service_modified = datetime.now()
        UtilityServiceSnapshot.generate(instance, service_modified=service_modified)
