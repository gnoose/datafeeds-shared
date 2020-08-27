"""
For debugging NVE Urjanet scraper, and surfacing which existing bills will be affected by your scraper changes.

Pull down nve-energy meters and bills and place these top-level in datafeeds repo.

test meters
\copy (select distinct sa.hex_id, m.oid, us.utility_account_id, us.service_id, mds.meta
from product_enrollment pe, snapmeter_account sa, snapmeter_account_meter sam, snapmeter_meter_data_source mds, meter m, utility_service us
where mds.name='nve-urjanet' and mds.meter=sam.meter and sam.account=sa.oid and sam.meter=pe.meter
 and pe.product='hodor' and pe.status <> 'notEnrolled' and sam.meter=m.oid and m.service=us.oid order by sa.hex_id)
 to '/builds/nv_meters062320.csv' DELIMITER ',' CSV HEADER;

test bill data
\copy (select distinct us.service_id, b.initial, b.closing, cost, used, peak, mds.meta, b.manual
from product_enrollment pe, snapmeter_meter_data_source mds, meter m, utility_service us, bill b
where mds.name='nve-urjanet' and mds.meter=pe.meter and pe.product='hodor' and pe.status <> 'notEnrolled'
and mds.meter=m.oid and m.service=us.oid and us.oid=b.service order by us.service_id, b.initial)
to '/builds/nv_bills062320.csv' DELIMITER ',' CSV HEADER;


- Temporary create an `out` directory to house the output.

Run:
$ python scripts/compare_nvenergyurja.py nve-urjanet nv_meters062320.csv nv_bills062320.csv

Conflicts written to out/nveurja_conflicts.csv

"""
import os
import re
import json
import argparse
from collections import namedtuple
import csv
from datetime import date, timedelta


from dateutil import parser as date_parser
import launch
import pymysql
from pymysql.cursors import DictCursor

from datafeeds import config


parser = argparse.ArgumentParser("Compare a local Urjanet run against test data.")
parser.add_argument("scraper", type=str)
parser.add_argument(
    "meters_file", type=str, help="csv with hex_id,oid,utility_account_id,service_id"
)
parser.add_argument(
    "bills_file", type=str, help="csv with service_id,initial,closing,cost,used,peak"
)

BillRow = namedtuple(
    "BillRow",
    ["service_id", "initial", "closing", "cost", "used", "peak", "meta", "manual"],
)
MeterRow = namedtuple(
    "MeterRow", ["hex_id", "oid", "utility_account_id", "service_id", "meta"]
)


def equal(val1, val2):
    v1 = float(val1 or 0)
    v2 = float(val2 or 0)
    if v2 == 0.0:
        return False
    return round(v1) == round(v2)


def print_row(prefix, bill):
    initial = date_parser.parse(bill.initial)
    closing = date_parser.parse(bill.closing)
    print(
        "{0}\t{1} - {2}\t{3:,}\t{4:,}\t{5:,}".format(
            prefix,
            initial.strftime("%m/%d/%Y"),
            closing.strftime("%m/%d/%Y"),
            round(float(bill.cost or 0)),
            round(float(bill.used or 0)),
            round(float(bill.peak or 0)),
        )
    )


def get_pdf_url(conn, start: date, end: date, account_id: str):
    query = """
        select PK, IntervalStart, IntervalEnd, SourceLink
        from Account
        where AccountNumber=%s and IntervalStart >= %s and IntervalStart <= %s
    """
    start2 = start + timedelta(days=1)
    with conn.cursor(DictCursor) as cursor:
        cursor.execute(
            query, (account_id, start.strftime("%Y-%m-%d"), start2.strftime("%Y-%m-%d"))
        )
        print(
            re.sub(r"\s+", " ", query)
            % (
                "'%s'" % account_id,
                start.strftime("'%Y-%m-%d'"),
                start2.strftime("'%Y-%m-%d'"),
            )
        )
        for row in cursor.fetchall():
            print(
                "PDF: %s - %s PK=%s %s"
                % (
                    row["IntervalStart"].strftime("%Y-%m-%d"),
                    row["IntervalEnd"].strftime("%Y-%m-%d"),
                    row["PK"],
                    row["SourceLink"],
                )
            )
            return row["SourceLink"]


def main():
    bad_accounts = []
    good_accounts = []

    args = parser.parse_args()

    mysql_conn = pymysql.connect(
        host=config.URJANET_MYSQL_HOST,
        user=config.URJANET_MYSQL_USER,
        passwd=config.URJANET_MYSQL_PASSWORD,
        db=config.URJANET_MYSQL_DB,
    )
    # load bills
    bills = {}
    print("loading bills from %s" % args.bills_file)
    with open(args.bills_file, "r") as f:
        reader = csv.reader(f)
        # service_id,initial,closing,cost,used,peak,meta
        for row in reader:
            said = row[0]
            if said == "service_id":
                continue
            bills.setdefault(said, set())
            bills[said].add(
                BillRow(
                    row[0], row[1], row[2], row[3], row[4], row[5], row[6] or {}, row[7]
                )
            )
    print("loaded bills for %s saids" % len(bills))

    meter_rows = []
    print("loading meters from %s" % args.meters_file)
    with open(args.meters_file, "r") as f:
        reader = csv.reader(f)
        # hex_id,oid,utility_account_id,service_id,meta
        for row in reader:
            if row[0] == "hex_id":
                continue
            meter_rows.append(MeterRow(*row))
    print("%s meters" % len(meter_rows))

    with open("out/nveurja_conflicts.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "PDF",
                "Command",
                "Meter",
                "Initial",
                "Urja Initial",
                "Closing",
                "Urja Closing",
                "db cost",
                "new cost",
                "db use",
                "new use",
                "db peak",
                "new peak",
                "Explanation",
            ]
        )
        for meter in meter_rows:
            url = "https://snapmeter.com/admin/accounts/%s/meters/%s/bills" % (
                meter.hex_id,
                meter.oid,
            )
            command = (
                "python launch.py by-name %s %s %s 2020-01-01 2020-06-01 --meta=%s"
                % (args.scraper, meter.utility_account_id, meter.service_id, meter.meta)
            )
            print(url)
            print(command)

            launch.launch_by_name(
                args.scraper,
                date(2020, 1, 1),
                date(2020, 6, 1),
                meter.utility_account_id,
                meter.service_id,
                username=None,
                password=None,
                meta=json.loads(meter.meta) if meter.meta else None,
                gen_service_id=None,
                source_type=None,
                exit=False,
            )
            # workdir/bills.csv: Service ID,Start,End,Cost,Used,Peak,Meta,Manual
            said = meter.service_id.replace(" ", "")
            urja_fn = "out/urja-%s.csv" % said
            if not os.path.exists("workdir/bills.csv"):
                print("no bills for meter %s" % meter.oid)
                continue
            os.rename("workdir/bills.csv", urja_fn)
            urja_bills = []
            with open(urja_fn, "r") as f:
                reader = csv.reader(f)
                for row in reader:
                    if row[0] == "Service ID":
                        continue
                    urja_bills.append(
                        BillRow(row[0], row[1], row[2], row[3], row[4], row[5], {}, "")
                    )

            said_bills = bills.get(meter.service_id, [])
            count = 0
            for urja_bill in urja_bills:
                match = [b for b in said_bills if b.closing == urja_bill.closing]
                if not match:
                    print(
                        "%s %s\turja bill not found"
                        % (urja_bill.initial, urja_bill.closing)
                    )
                    continue
                bill = match[0]
                if bill.manual == "f":
                    # source, dates, cost, used, peak
                    mismatches = [""] * 5
                    db_used = int(float(bill.used or "0"))
                    db_peak = int(float(bill.peak or "0"))
                    urja_used = int(float(urja_bill.used or "0"))
                    urja_peak = int(float(urja_bill.peak or "0"))
                    if not urja_bill.initial == bill.initial:
                        mismatches[1] = "dates"
                    if not urja_bill.closing == bill.closing:
                        mismatches[1] = "dates"
                    if not equal(urja_bill.cost, bill.cost):
                        mismatches[2] = "cost"
                    if not equal(urja_bill.used, bill.used) and db_used > 0:
                        mismatches[3] = "used"
                    if not equal(urja_bill.peak, bill.peak) and db_peak > 0:
                        mismatches[4] = "peak"
                    if (
                        "".join(mismatches)
                        or (db_used == 0 and urja_used > 0)
                        or (db_peak == 0 and urja_peak > 0)
                    ):
                        count += 1
                        print_row("db  ", bill)
                        print_row("urja", urja_bill)
                        print("mismatch", "\t".join(mismatches))
                        urja_start = date_parser.parse(urja_bill.initial) - timedelta(
                            days=1
                        )
                        urja_end = date_parser.parse(urja_bill.closing) + timedelta(
                            days=1
                        )
                        pdf_url = get_pdf_url(
                            mysql_conn, urja_start, urja_end, meter.utility_account_id
                        )
                        writer.writerow(
                            [
                                pdf_url,
                                command,
                                url,
                                bill.initial,
                                urja_bill.initial,
                                bill.closing,
                                urja_bill.closing,
                                bill.cost,
                                urja_bill.cost,
                                bill.used,
                                urja_bill.used,
                                bill.peak,
                                urja_bill.peak,
                            ]
                        )

            if not count:
                print("all bills match ✨")
                good_accounts.append(
                    "https://snapmeter.com/admin/accounts/%s/meters/%s/bills"
                    % (meter.hex_id, meter.oid)
                )

            else:
                bad_accounts.append(
                    {
                        "https://snapmeter.com/admin/accounts/%s/meters/%s/bills"
                        % (meter.hex_id, meter.oid): count
                    }
                )
    print("Bills match:")
    for account in good_accounts:
        print(account)

    print("Issues found:")
    for account in bad_accounts:
        print(account)


if __name__ == "__main__":
    main()
