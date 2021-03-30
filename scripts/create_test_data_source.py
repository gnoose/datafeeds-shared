import argparse
from datetime import datetime, timedelta

from datafeeds import db
from datafeeds.models import (
    SnapmeterAccountDataSource as AccountDataSource,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.models.account import SnapmeterAccount
from datafeeds.models.meter import Meter, Building
from datafeeds.models.utility_service import UtilityService


parser = argparse.ArgumentParser("Add a new meter, utility service, and datasource")
parser.add_argument("datasource")
parser.add_argument("username")
parser.add_argument("password")
parser.add_argument("--utility_account_id")
parser.add_argument("--service_id")
parser.add_argument("--gen_service_id")


def main():
    args = parser.parse_args()
    account = (
        db.session.query(SnapmeterAccount).filter_by(name="Dev Test Account").first()
    )
    building = db.session.query(Building).filter_by(oid=1).first()
    # create meter and utility_service
    utility_service = UtilityService(
        service_id=args.service_id,
        account_id=args.utility_account_id,
        gen_service_id=args.gen_service_id,
    )
    utility_service.active = True
    utility_service.tariff = "B-1"
    utility_service.utility = "utility:pge"
    db.session.add(utility_service)
    db.session.flush()
    meter = Meter(
        "Meter %s" % (datetime.now().strftime("%Y%m%d%H%M")),
        building=building,
        utility_service=utility_service,
    )
    db.session.add(meter)
    db.session.flush()
    account.meters.append(meter)
    db.session.add(account)
    print("Created meter %s" % meter.oid)

    is_urjanet = "urjanet" in args.datasource
    # Urjanet data sources are meter-only.
    ads = None
    if not is_urjanet:
        ads = AccountDataSource(
            account=account,
            source_account_type=args.datasource,
            name="%s - %s" % (args.datasource, args.username),
        )
        ads.encrypt_username(args.username)
        ads.encrypt_password(args.password)
        db.session.add(ads)
        db.session.flush()
        print("Created account data source %s" % ads.oid)

    mds = MeterDataSource(meter=meter, name=args.datasource)
    mds.account_data_source = ads
    db.session.add(mds)
    db.session.flush()
    print("Created meter data source %s" % mds.oid)
    print(
        "\nRun scraper: python launch.py by-oid %s %s %s"
        % (
            mds.oid,
            (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d"),
            datetime.now().strftime("%Y-%m-%d"),
        )
    )
    db.session.commit()


if __name__ == "__main__":
    db.init()
    main()
