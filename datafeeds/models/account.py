"""Account

This module covers tables managed by webapps that describe Snapmeter accounts.
Except for unit tests, analytics should treat these tables as Read Only.

We need this in order to be able to work with complete "Snapmeter Building" objects. There is
likely a refactor opportunity here to eliminate this module.
"""

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import BigInteger, Boolean, Column, DateTime, Enum, ForeignKey, Integer, Unicode
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.sql import func
from sqlalchemy.ext.mutable import MutableDict

from datafeeds.orm import ModelMixin, Base


GENERATION_PROVIDERS = ["utility-bundled", "tnd-only", "cca"]


class SnapmeterAccount(ModelMixin, Base):
    __tablename__ = "snapmeter_account"

    oid = Column(BigInteger, primary_key=True)
    hex_id = Column(Unicode)
    account_type = Column(Unicode, nullable=False, default="free")
    created = Column(DateTime, nullable=False)
    domain = Column(Unicode, nullable=False, default="gridium.com")
    name = Column(Unicode, nullable=False)
    status = Column(Unicode, nullable=False, default="setup")
    token_login = Column(Boolean, nullable=False, default=True)

    meters = association_proxy("snapmeter_account_meters", "meter_obj",
                               creator=lambda m: SnapmeterAccountMeter(meter_obj=m))


class SnapmeterAccountMeter(ModelMixin, Base):
    __tablename__ = "snapmeter_account_meter"

    oid = Column(Integer, primary_key=True)
    account = Column(BigInteger, ForeignKey("snapmeter_account.oid"))
    meter = Column(BigInteger, ForeignKey("meter.oid"))
    utility_account_id = Column(Unicode)
    estimated_changes = Column(MutableDict.as_mutable(JSONB))
    created = Column(DateTime, default=func.now())
    generation_provider = Column(Enum(*GENERATION_PROVIDERS), default=GENERATION_PROVIDERS[0])
    snapmeter_delivery = Column(Boolean, nullable=False, default=True)

    account_obj = relationship(SnapmeterAccount, backref=backref("snapmeter_account_meters"))
    meter_obj = relationship("Meter", back_populates="snapmeter_account_meter")
