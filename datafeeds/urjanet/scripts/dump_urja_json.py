"""A simple script for dumping urjanet data into a json format"""

import argparse
import json
import random
import re
import sys

import pymysql

from datafeeds import config
from datafeeds.urjanet.scripts.cli_hooks import get_cli_hooks
from datafeeds.urjanet.transformer import urja_to_json
from datafeeds.urjanet.model import order_json


def _anonymize_number(num, replacements=None):
    if replacements is None:
        replacements = {}
    for i in range(10):
        if not i in replacements:
            replacements[i] = str(random.randrange(10))
        num = re.sub(str(i), replacements[i], num)
    return num


def fetch_data(datasource):
    data = datasource.load()
    # anonymize numbers but keep structure (spaces, dashes, etc)
    for account in data.accounts:
        replacements = {}
        account.AccountNumber = _anonymize_number(account.AccountNumber, replacements)
        account.RawAccountNumber = _anonymize_number(
            account.RawAccountNumber, replacements
        )

        for meter in account.meters:
            meter.PODid = _anonymize_number(meter.PODid)
            meter.MeterNumber = _anonymize_number(meter.MeterNumber)

    return urja_to_json(data)


def write_data(writer, data):
    writer.write(json.dumps(order_json(data), indent=4))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--outfile", help="The file to write to (else stdout is used)")

    subparsers = parser.add_subparsers()
    for _, hook_cls in get_cli_hooks().items():
        hook_cls().add_subparser(subparsers)

    args = parser.parse_args()
    if not hasattr(args, "datasource_cli"):
        parser.error("Specify a subcommand.")

    conn = pymysql.connect(
        host=config.URJANET_MYSQL_HOST,
        user=config.URJANET_MYSQL_USER,
        passwd=config.URJANET_MYSQL_PASSWORD,
        db=config.URJANET_MYSQL_DB,
    )

    writer = None
    try:
        datasource = args.datasource_cli.make_datasource(conn, args)
        urja_data = fetch_data(datasource)

        writer = sys.stdout
        if args.outfile:
            writer = open(args.outfile, "w")
        write_data(writer, urja_data)
    finally:
        if conn:
            conn.close()
        if writer and args.outfile:
            writer.close()


if __name__ == "__main__":
    main()
