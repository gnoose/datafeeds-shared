""" User

This module covers tables managed by webapps that describe users and related data.
Except for unit tests, datafeeds should treat these tables as read only.
"""


import sqlalchemy as sa
from sqlalchemy import not_, cast
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from datafeeds import db
from datafeeds.models import SnapmeterAccountMeter
from datafeeds.orm import ModelMixin, Base


class SnapmeterUser(ModelMixin, Base):
    __tablename__ = "snapmeter_user"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    hex_id = sa.Column(sa.Unicode)
    email = sa.Column(sa.Unicode)
    password = sa.Column(sa.Unicode)
    name = sa.Column(sa.Unicode)
    groups = sa.Column(ARRAY(sa.Unicode))
    meta = sa.Column(JSONB)


class SnapmeterAccountUser(ModelMixin, Base):
    __tablename__ = "snapmeter_account_user"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    user = sa.Column(sa.BigInteger, sa.ForeignKey("snapmeter_user.oid"))
    account = sa.Column(sa.BigInteger, sa.ForeignKey("snapmeter_account.oid"))

    @classmethod
    def account_user_count(cls, meter_oid: int) -> int:
        """Get the number of external users with access to this account."""
        return (
            db.session.query(SnapmeterAccountUser)
            .filter(
                SnapmeterAccountUser.account == SnapmeterAccountMeter.account,
                SnapmeterAccountMeter.meter == meter_oid,
                SnapmeterAccountUser.user == SnapmeterUser.oid,
                not_(
                    SnapmeterUser.groups.overlap(
                        cast(
                            ["groups:superusers", "groups:ce-snapmeter"],
                            ARRAY(sa.Unicode),
                        )
                    )
                ),
            )
            .count()
        )


class SnapmeterUserSubscription(ModelMixin, Base):
    __tablename__ = "snapmeter_user_subscription"

    oid = sa.Column(sa.BigInteger, primary_key=True)

    user = sa.Column(sa.BigInteger, sa.ForeignKey("snapmeter_user.oid"))
    subscription = sa.Column(sa.Unicode)
    meter = sa.Column(sa.BigInteger, sa.ForeignKey("meter.oid"))
    sent = sa.Column(sa.DateTime)

    @classmethod
    def email_subscriber_count(cls, meter_oid: int) -> int:
        """Get the number of external users with a subscription to this meter."""
        return (
            db.session.query(SnapmeterUserSubscription)
            .filter(
                SnapmeterUserSubscription.meter == meter_oid,
                SnapmeterUserSubscription.user == SnapmeterUser.oid,
                not_(
                    SnapmeterUser.groups.overlap(
                        cast(
                            ["groups:superusers", "groups:ce-snapmeter"],
                            ARRAY(sa.Unicode),
                        )
                    )
                ),
            )
            .count()
        )
