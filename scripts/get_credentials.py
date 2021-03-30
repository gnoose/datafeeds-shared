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


parser = argparse.ArgumentParser("Get credentials for a datasource")
parser.add_argument("datasource", type=int)


def main():
    args = parser.parse_args()
    mds = db.session.query(MeterDataSource).get(args.datasource)
    if not mds:
        print("Meter datasource %s not found" % args.datasource)
        return
    if not mds.account_data_source:
        print("Account datasource for meter datasource %s not found" % args.datasource)
    print(
        "%s %s" % (mds.account_data_source.username, mds.account_data_source.password)
    )


if __name__ == "__main__":
    db.init()
    main()
