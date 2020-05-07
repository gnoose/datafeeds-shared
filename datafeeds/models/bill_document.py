"""BillDocument

This module covers tables managed by webapps that describe utility services.
Except for unit tests, analytics should treat these tables as Read Only.
"""
from datetime import datetime

from dateutil import parser as date_parser

from datafeeds import db
from datafeeds.common.typing import AttachmentEntry
from datafeeds.orm import ModelMixin, Base

import sqlalchemy as sa


class BillDocument(ModelMixin, Base):
    """A bill document (usually PDF, but also CSV).

    It is associated with a utility account id, and may contain data for multiple services.
    """

    __tablename__ = "bill_document"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    s3_key = sa.Column(sa.Unicode, nullable=False)
    # pdf, csv, etc
    doc_format = sa.Column(sa.Unicode, nullable=False)
    # Gridium utility, without utility: prefix
    utility = sa.Column(sa.Unicode, nullable=False)
    utility_account_id = sa.Column(sa.Unicode, nullable=False)
    # if the document contains data from a different utility for generation
    gen_utility = sa.Column(sa.Unicode)
    gen_utility_account_id = sa.Column(sa.Unicode)
    # statement date (if available; may be the same as bill closing date)
    statement_date = sa.Column(sa.Date, nullable=False)
    # where we acquired this document (utility website, Urjanet, etc)
    source = sa.Column(sa.Unicode)
    created = sa.Column(sa.DateTime, nullable=False)

    @classmethod
    def add_or_update(cls, entry: AttachmentEntry):
        """Add or update a bill document record. s3_key is a unique key."""
        doc = db.session.query(BillDocument).filter_by(s3_key=entry.key).first()
        statement_date = None
        if entry.statement:
            statement_date = date_parser.parse(entry.statement)
        gen_utility = entry.gen_utility
        if gen_utility:
            gen_utility = gen_utility.replace("utility:", "")
        if doc:
            if statement_date:
                doc.statement_date = statement_date
            doc.doc_format = entry.format
            doc.utility = entry.utility.replace("utility:", "")
            doc.utility_account_id = entry.utility_account_id
            doc.gen_utility = gen_utility
            doc.gen_utility_account_id = entry.gen_utility_account_id
            doc.source = entry.source
        else:
            doc = BillDocument(
                s3_key=entry.key,
                doc_format=entry.format,
                utility=entry.utility.replace("utility:", ""),
                utility_account_id=entry.utility_account_id,
                gen_utility=gen_utility,
                gen_utility_account_id=entry.gen_utility_account_id,
                statement_date=statement_date,
                source=entry.source,
                created=datetime.now(),
            )
        db.session.add(doc)
