from datetime import datetime
import os
from functools import lru_cache
import logging
from typing import Tuple, List
import uuid

import boto3
import botocore
from sqlalchemy_utils.functions import database_exists, create_database

from datafeeds.config import DATAFEEDS_ROOT
from datafeeds import db, config
from datafeeds.models import (
    SnapmeterAccount,
    SnapmeterAccountDataSource,
    SnapmeterMeterDataSource,
)
from datafeeds.models import Meter, UtilityService


log = logging.getLogger(__name__)


CONNSTR = "postgresql+psycopg2://postgres@pg/gridium_test"


def init_test_db():
    """Create a connection to the test database.

    Create if needed, using a static fixture file. Update fixtures/schema.sql for any schema
    changes that affect datafeeds.
    """
    exists = database_exists(CONNSTR)
    if not exists:
        create_database(CONNSTR)

    db.init(CONNSTR, statement_timeout=10000)

    def source_sql_file(pathname):
        with open(os.path.join(DATAFEEDS_ROOT, pathname)) as f:
            sql = f.read()
        with db.engine.begin() as connection:
            connection.execute(sql)

    if not exists:
        # turn off echo for now
        old_echo_value = db.engine.echo
        db.engine.echo = False

        # create non-SQLAlchemy tables
        source_sql_file("fixtures/init_test.sql")
        source_sql_file("fixtures/schema.sql")

        db.engine.echo = old_echo_value


def create_meters() -> Tuple[SnapmeterAccount, List[Meter]]:
    """Create an account and two meter objects."""
    account = SnapmeterAccount(
        hex_id=uuid.uuid4().hex,
        name="Test Account %s" % datetime.now().strftime("%s"),
        account_type="free",
        created=datetime.now(),
        domain="gridium.com",
        status="setup",
        token_login=True,
    )
    db.session.add(account)
    # create two test meters
    us = UtilityService(service_id=datetime.now().strftime("%s%f"))
    us.utility_account_id = datetime.now().strftime("%s%f")
    db.session.add(us)
    db.session.flush()
    meter1 = Meter(
        commodity="kw",
        interval=15,
        kind="main",
        name="Test Meter 1-%s" % datetime.now().strftime("%s"),
        utility_service=us,
    )
    db.session.add(meter1)
    us = UtilityService(service_id=datetime.now().strftime("%s%f"))
    us.utility_account_id = datetime.now().strftime("%s%f")
    db.session.add(us)
    db.session.flush()
    meter2 = Meter(
        commodity="kw",
        interval=15,
        kind="main",
        name="Test Meter 2-%s" % datetime.now().strftime("%s"),
        utility_service=us,
    )
    db.session.add(meter2)
    db.session.flush()
    account.meters.append(meter1)
    account.meters.append(meter2)
    db.session.flush()

    return account, [meter1, meter2]


def add_datasources(account: SnapmeterAccount, meters: List[Meter], name: str) -> None:
    """Create an account data source and add meter data data sources."""
    # create one account data source
    ads = SnapmeterAccountDataSource(
        account=account, source_account_type=name, name="%s - Test" % name, enabled=True
    )
    ads.encrypt_username("1234567")
    ads.encrypt_password("1234567")
    db.session.add(ads)
    db.session.flush()
    # create meter data source for each meter, using the account data source
    for meter in meters:
        db.session.add(
            SnapmeterMeterDataSource(
                meter=meter,
                name=name,
                account_data_source=ads,
                source_types=["interval"],
                meta={"test": "abc"},  # extra meta we want to preserve
            )
        )
    db.session.flush()


s3 = boto3.resource("s3")


class FixtureNotFoundError(Exception):
    pass


@lru_cache(maxsize=32)
def private_fixture(s3_key: str) -> bytes:
    path = os.path.join(config.PRIVATE_FIXTURES_PATH, s3_key)

    if not os.path.exists(path):
        try:
            log.debug("Downloading test fixture %s from S3.", s3_key)
            s3.Bucket(config.PRIVATE_FIXTURES_S3_BUCKET).download_file(s3_key, path)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise FixtureNotFoundError("Could not locate test fixture %s." % s3_key)
            else:
                raise

    with open(path, "rb") as fixture:
        log.debug("Reading test fixture %s from disk cache.", s3_key)
        return fixture.read()
