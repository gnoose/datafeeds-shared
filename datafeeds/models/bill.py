"""Bill

This module covers tables managed by webapps/platform that describe utility services.
Except for unit tests, analytics should treat these tables as Read Only.
"""

from sqlalchemy import JSON

from datafeeds.orm import ModelMixin, Base

import sqlalchemy as sa


class Bill(ModelMixin, Base):
    __tablename__ = "bill"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    attachments = sa.Column(JSON)
    closing = sa.Column(sa.Date)
    cost = sa.Column(sa.Float)
    initial = sa.Column(sa.Date)
    items = sa.Column(JSON)
    manual = sa.Column(sa.Boolean)
    modified = sa.Column(sa.DateTime)
    peak = sa.Column(sa.Float)
    service = sa.Column(sa.BigInteger)
    used = sa.Column(sa.Float)
    notes = sa.Column(sa.Unicode)
    visible = sa.Column(sa.Boolean, nullable=False, default=True)
