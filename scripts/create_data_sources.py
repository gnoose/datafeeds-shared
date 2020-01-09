import argparse

from datafeeds import db

from datafeeds.models import (
    SnapmeterAccountDataSource as AccountDataSource,
    SnapmeterMeterDataSource as MeterDataSource,
)


parser = argparse.ArgumentParser("Add a new meter data source.")
parser.add_argument("account_oid", type=int)
parser.add_argument("meter_oid", type=int)
parser.add_argument("source_account_type")
parser.add_argument("name")


def main():
    args = parser.parse_args()
    is_urjanet = "urjanet" in args.source_account_type

    # Urjanet data sources are meter-only.
    if not is_urjanet:
        ads = AccountDataSource(
            _account=args.account_oid,
            source_account_type=args.source_account_type,
            name=args.name,
        )

        db.session.add(ads)
        db.session.flush()
        print("Added Account Data Source.")

    mds = MeterDataSource(_meter=args.meter_oid, name=args.source_account_type)
    if not is_urjanet:
        mds.account_data_source = ads
    db.session.add(mds)
    db.session.flush()
    print("Added Meter Data Source %s." % mds.oid)
    print("Done.")
    db.session.commit()


if __name__ == "__main__":
    db.init()
    main()
