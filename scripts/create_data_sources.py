import argparse

from datafeeds import db

from datafeeds.models import \
    SnapmeterAccountDataSource as AccountDataSource, \
    SnapmeterMeterDataSource as MeterDataSource


parser = argparse.ArgumentParser("Add a new meter data source.")
parser.add_argument("account_oid", type=int)
parser.add_argument("meter_oid", type=int)
parser.add_argument("source_account_type")
parser.add_argument("name")


def main():
    args = parser.parse_args()
    ads = AccountDataSource(_account=args.account_oid,
                            source_account_type=args.source_account_type,
                            name=args.name)

    db.session.add(ads)
    db.session.flush()
    print("Added Account Data Source.")

    mds = MeterDataSource(_meter=args.meter_oid, name=args.source_account_type)
    mds.account_data_source = ads
    db.session.add(mds)
    db.session.flush()
    print("Added Meter Data Source.")
    print("Done.")
    db.session.commit()


if __name__ == '__main__':
    db.init()
    main()
