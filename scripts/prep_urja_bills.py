import argparse
import csv
import os
import re

import requests
import pymysql
from pymysql.cursors import DictCursor

from datafeeds import config


"""
Run this script to prepare Urjanet bills for data capture.

python scripts/prep_urja_bills.py EBMUD --accounts 4 --size 6

Create utility directory in Gridium / Scraper development
  - https://drive.google.com/drive/folders/1Ze-ByPO9-331IYaWyXoZpTN_rDzbyUem

Copy worksheet and data capture from another utility.

Import utility/bills.csv to utility worksheet 1
  - Replace current sheet
  - Convert text to numbers, dates, and formulas - No (to preserve leading spaces)

Set up formatting.

Copy to worksheet 2.

Upload PDFs.

Update Data capture instructions with screenshot.
"""


def main(utility: str, accounts: int, size: int, ignore_meters: bool):
    conn = pymysql.connect(
        host=config.URJANET_MYSQL_HOST,
        user=config.URJANET_MYSQL_USER,
        passwd=config.URJANET_MYSQL_PASSWORD,
        db=config.URJANET_MYSQL_DB,
    )
    bills = {}
    if ignore_meters:
        query = """
            select distinct AccountNumber, '' as MeterNumber, SourceLink, StatementDate
            from Account a
            where a.UtilityProvider=%s
            order by StatementDate desc
        """
    else:
        query = """
            select distinct AccountNumber, MeterNumber, SourceLink, StatementDate
            from Account a, Meter m
            where a.UtilityProvider=%s and a.PK=m.AccountFK
            order by StatementDate desc
        """
    total = 0
    with conn.cursor(DictCursor) as cursor:
        cursor.execute(query, utility)
        for row in cursor.fetchall():
            if row["AccountNumber"] not in bills and len(bills.keys()) >= accounts:
                continue
            bills.setdefault(row["AccountNumber"], [])
            if len(bills[row["AccountNumber"]]) >= size:
                continue
            # get id from URL
            # https://sources.o2.urjanet.net/sourcewithhttpbasicauth?id=1e88f643-0dbe-d96f-b450-22000aa6a8a4
            row["id"] = re.match(r".*?id=(.*)", row["SourceLink"]).group(1)
            bills[row["AccountNumber"]].append(row)
            total += 1
    print("loaded %s bills\n" % total)
    print("downloading bill PDFs to %s" % utility)
    try:
        os.mkdir(utility)
    except FileExistsError:
        pass
    filename = "%s/bills.csv" % utility
    print("writing bills to %s" % filename)
    downloaded = set()
    with open(filename, "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "utility",
                "utility_account_id",
                "service_id",
                "start",
                "end",
                "cost",
                "used",
                "peak",
                "filename",
                "statement",
            ]
        )
        for account_id in bills:
            for row in bills[account_id]:
                pdf_filename = "%s.pdf" % (row["id"])
                if row["SourceLink"] not in downloaded:
                    print(
                        "downloading %s for %s %s"
                        % (pdf_filename, row["AccountNumber"], row["StatementDate"])
                    )
                    pdf = requests.get(
                        row["SourceLink"],
                        auth=(config.URJANET_HTTP_USER, config.URJANET_HTTP_PASSWORD),
                    )
                    if pdf.status_code == 200:
                        with open("%s/%s" % (utility, pdf_filename), "wb") as f:
                            f.write(pdf.content)
                        downloaded.add(row["SourceLink"])
                    else:
                        print(
                            "error downloading %s: %s"
                            % (row["SourceLink"], pdf.status_code)
                        )
                writer.writerow(
                    [
                        utility,
                        row["AccountNumber"],
                        row["MeterNumber"],
                        "",
                        "",
                        "",
                        "",
                        "",
                        pdf_filename,
                        row["StatementDate"],
                    ]
                )
    print("wrote %s" % filename)


def test():
    main("EBMUD", 2, 3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Describe and download Urjanet bills")
    parser.add_argument("utility", help="Urjanet utility code, ie EBMUD")
    parser.add_argument(
        "--accounts",
        type=int,
        default=4,
        help="number of accounts to return; default 4",
    )
    parser.add_argument(
        "--ignore-meters",
        dest="ignore_meters",
        action="store_const",
        const=True,
        help="ignore meter numbers and create one bill per account",
    )
    parser.add_argument(
        "--size",
        type=int,
        default=6,
        help="number of bills to return per account; default 6",
    )
    args = parser.parse_args()
    main(args.utility, args.accounts, args.size, args.ignore_meters)
