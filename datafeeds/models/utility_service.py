"""Utility Service

This module covers tables managed by webapps/platform that describe utility services.
Except for unit tests, analytics should treat these tables as Read Only.
"""

import sqlalchemy as sa
from sqlalchemy.orm import relationship

from datafeeds.orm import ModelMixin, Base


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

    meter = relationship("Meter", back_populates="utility_service")

    # Don't use this constructor outside of tests.
    def __init__(self, service_id: str):
        self.oid = UtilityService.get_new_oid()
        self.service_id = service_id

