"""Utility Service

This module covers tables managed by webapps/platform that describe utility services.
Except for unit tests, analytics should treat these tables as Read Only.
"""
from datetime import datetime
from typing import Optional
import sqlalchemy as sa
from sqlalchemy.orm import relationship

from datafeeds.orm import ModelMixin, Base


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
        self.oid = UtilityService.get_new_oid()
        self.service_id = service_id
        self.utility_account_id = account_id
        self.gen_service_id = gen_service_id


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
