"""A simple script for transforming Urjanet json (e.g. from `dump_pge_json.py`) into billing periods"""

import csv
import sys
import json
import argparse
import logging
from datetime import datetime

from datafeeds import config
from datafeeds.urjanet.model import UrjanetData, order_json
from datafeeds.urjanet.scripts.cli_hooks import get_cli_hooks

logging.config.dictConfig(config.LOGGING)


def process_json(args):
    with open(args.path) as f:
        json_dict = json.load(f)
        urja_data = UrjanetData(json_dict)
    transformer = args.transformer
    result = transformer.urja_to_gridium(urja_data)

    outstream = sys.stdout
    opened_file = False
    if args.outfile:
        outstream = open(args.outfile, "w")
        opened_file = True

    try:
        if args.csv:
            writer = csv.writer(outstream)
            for item in result.periods:
                writer.writerow([item.start, item.end, item.total_charge, item.total_usage, item.peak_demand])
        else:
            json_data = result.to_json()
            ordered = order_json(json_data)
            outstream.write(json.dumps(ordered, indent=4))
    finally:
        if opened_file and outstream:
            outstream.close()


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Path to json-serialized billing data")
    parser.add_argument("--csv", action="store_true")
    parser.add_argument("--outfile")
    parser.add_argument("-v", "--verbose", action="store_true")

    subparsers = parser.add_subparsers()
    for key, hook_cls in get_cli_hooks().items():
        transformer = hook_cls().make_transformer()
        sub = subparsers.add_parser(key)
        sub.set_defaults(transformer=transformer)

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger("datafeeds").setLevel(level=logging.DEBUG)

    process_json(args)


if __name__ == "__main__":
    main()
