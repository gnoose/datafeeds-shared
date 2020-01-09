import sqlalchemy as sa
from sqlalchemy.orm import relationship, backref
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
import pyaes

from datafeeds.orm import ModelMixin, Base
from datafeeds import db, config


def _aes_key():
    # key must be exactly 32 bytes
    return (config.AES_KEY or "snapmeter" * 4)[:32].encode("utf-8")


def aes_encrypt(text):
    """encrypt the given string and return bytes"""
    aes = pyaes.AESModeOfOperationCTR(_aes_key())
    return aes.encrypt(text)  # bytes


def aes_decrypt(encrypted_bytes):
    """decrypt the given bytes and return a string"""
    aes = pyaes.AESModeOfOperationCTR(_aes_key())
    return aes.decrypt(encrypted_bytes).decode("utf-8")


class SnapmeterAccountDataSource(ModelMixin, Base):
    __tablename__ = "snapmeter_account_data_source"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    hex_id = sa.Column(sa.Unicode)
    _account = sa.Column(
        "account", sa.BigInteger, sa.ForeignKey("snapmeter_account.oid")
    )
    account = relationship("SnapmeterAccount")
    source_account_type = sa.Column(sa.Unicode)
    name = sa.Column(sa.Unicode)
    # set this via sys_acct.encrypt_username
    # get unencrypted value via sys_acct.username
    _username_bytes = sa.Column("username_bytes", sa.LargeBinary)
    # set this via sys_acct.encrypt_password
    # get unencrypted value via sys_acct.password
    _password_bytes = sa.Column("password_bytes", sa.LargeBinary)
    enabled = sa.Column(sa.Boolean, default=True)

    # One-to-many SnapmeterMeterDataSource with backref, eg. meter_data_source.account_ds
    # Defaults to lazy-loading when loading either side of the relationship.
    meter_data_sources = relationship(
        "SnapmeterMeterDataSource", backref=backref("account_ds")
    )  # FIXME

    @classmethod
    def by_hex_id(cls, hex_id):
        return (
            db.session.query(SnapmeterAccountDataSource)
            .filter_by(hex_id=hex_id)
            .first()
        )

    @classmethod
    def by_account_and_type(cls, account_oid, account_type, username=None):
        query = db.session.query(SnapmeterAccountDataSource).filter_by(
            account=account_oid, source_account_type=account_type
        )
        if username:
            query = query.filter_by(_username_bytes=aes_encrypt(username))
        return query.first()

    @property
    def username(self):
        if not self._username_bytes:
            return None
        return aes_decrypt(self._username_bytes)

    def encrypt_password(self, password):
        self._password_bytes = aes_encrypt(password) if password else None

    @property
    def password(self):
        if not self._password_bytes:
            return None
        return aes_decrypt(self._password_bytes)

    def encrypt_username(self, username):
        self._username_bytes = aes_encrypt(username) if username else None

    def to_json(self, include_password=False, str_ids=False):
        # stringify ids to avoid JSON parsing rollover issues
        rval = {
            "id": str(self.oid) if str_ids else self.oit,
            "name": self.name,
            "accountType": self.source_account_type,
            "username": self.username,
            "accountId": str(self.account) if str_ids else self.account,
        }
        if include_password:
            rval["password"] = self.password
        return rval


class SnapmeterMeterDataSource(ModelMixin, Base):
    __tablename__ = "snapmeter_meter_data_source"

    oid = sa.Column(sa.BigInteger, primary_key=True)
    hex_id = sa.Column(sa.Unicode)
    _meter = sa.Column(
        "meter", sa.BigInteger, sa.ForeignKey("meter.oid")
    )  # Not a real foreign key constraint.
    meter = relationship("Meter")

    name = sa.Column(sa.Unicode)
    _account_data_source = sa.Column(
        "account_data_source",
        sa.BigInteger,
        sa.ForeignKey("snapmeter_account_data_source.oid"),
    )
    account_data_source = relationship("SnapmeterAccountDataSource")
    meta = sa.Column(JSONB)
    source_types = sa.Column(ARRAY(sa.Unicode))

    @classmethod
    def by_meter_and_type(cls, meter_id, source_type):
        return (
            db.session.query(SnapmeterMeterDataSource)
            .filter_by(meter=meter_id)
            .filter(SnapmeterMeterDataSource.source_types.any(source_type))
            .one_or_none()
        )

    @classmethod
    def by_meter_and_name(cls, meter_id, name):
        return (
            db.session.query(SnapmeterMeterDataSource)
            .filter_by(meter=int(meter_id), name=name)
            .one_or_none()
        )

    def to_json(self, account_id=None, include_credentials=False, str_ids=False):
        rval = {
            "id": str(self.oid) if str_ids else self.oid,
            "name": self.name,
            "meter": str(self.meter) if str_ids else self.meter,
            "meta": self.meta or {},
            "sourceTypes": self.source_types,
            "accountDataSource": None,
            "enabled": self.enabled,
        }
        if self.account_data_source:
            rval["accountDataSource"] = (
                str(self.account_data_source) if str_ids else self.account_data_source
            )

        if account_id:
            rval["account"] = str(account_id) if str_ids else account_id
        if include_credentials and self.account_data_source:
            rval["username"] = self.account_ds.username
            rval["password"] = self.account_ds.password
            rval["accountId"] = (
                str(self.account_ds.account) if str_ids else self.account_ds.account
            )
        return rval
