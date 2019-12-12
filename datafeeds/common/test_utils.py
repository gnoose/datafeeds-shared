from datetime import datetime
from typing import Tuple, List
import uuid

from datafeeds import db
from datafeeds.models import SnapmeterAccount, SnapmeterAccountDataSource, SnapmeterMeterDataSource
from datafeeds.models import Meter, UtilityService


def init_test_db():
    """Create a connection to the test database.

    This database should be created and upgraded by webapps. Run a webapps test to apply migrations
    to the test database if needed.
    """
    db.init("postgresql+psycopg2://postgres@pg/gridium_test")


def create_meters() -> Tuple[SnapmeterAccount, List[Meter]]:
    """Create an account and two meter objects."""
    account = SnapmeterAccount(
        hex_id=uuid.uuid4().hex,
        name="Test Account %s" % datetime.now().strftime("%s"),
        account_type="free",
        created=datetime.now(),
        domain="gridium.com",
        status="setup",
        token_login=True
    )
    db.session.add(account)
    # create two test meters
    us = UtilityService(service_id=datetime.now().strftime("%s%f"))
    db.session.add(us)
    db.session.flush()
    meter1 = Meter(
        commodity="kw",
        interval=15,
        kind="main",
        name="Test Meter 1-%s" % datetime.now().strftime("%s"),
        utility_service=us
    )
    db.session.add(meter1)
    us = UtilityService(service_id=datetime.now().strftime("%s%f"))
    db.session.add(us)
    db.session.flush()
    meter2 = Meter(
        commodity="kw",
        interval=15,
        kind="main",
        name="Test Meter 2-%s" % datetime.now().strftime("%s"),
        utility_service=us
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
        account=account,
        source_account_type=name,
        name="%s - Test" % name,
        enabled=True
    )
    ads.encrypt_username("1234567")
    ads.encrypt_password("1234567")
    db.session.add(ads)
    db.session.flush()
    # create meter data source for each meter, using the account data source
    for meter in meters:
        db.session.add(SnapmeterMeterDataSource(
            meter=meter,
            name=name,
            account_data_source=ads,
            source_types=["interval"],
            meta={"test": "abc"}  # extra meta we want to preserve
        ))
    db.session.flush()
