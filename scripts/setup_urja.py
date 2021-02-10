import argparse
import csv
import json
import os
from pydoc import locate
import random
import re
from typing import Any, Dict

import pymysql

from datafeeds import config
from datafeeds.urjanet.transformer import urja_to_json

"""
Use this script to set up a new Urjanet scraper.

Create datasource, transformer, and test classes: python setup_urja.py classes utility-id UrjanetUtilityProvider

    example: python setup_urja.py classes contra-costa-water ContraCostaWaterDistrictCA --water

Create test data: python setup_urja.py tests utility-id
Before running, put expected bill values in datafeeds/urjanet/tests/data/utility-id.csv

    example: python setup_urja.py tests ebmud
"""


def _anonymize_number(num, replacements=None):
    if replacements is None:
        replacements = {}
    for i in range(10):
        if i not in replacements:
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


def generate_classes(
    utility_id: str,
    utility_provider: str,
    utility_name: str,
    utility_filename: str,
    suffix: str,
):
    """Generate datasource and transformer from templates.

    _UtilityName_ = title case version of utility (ContraCostaWater)
    _UtilityProvider_ = Urjanet utility provider (ContraCostaWaterDistrictCA)
    _UtilityId_ = utility identifier (contra-costa-water)
    """

    # Create a datasource class from a template.
    with open("templates/urjanet_datasource%s.py" % suffix) as inf:
        data = inf.read()
    filename = "../datafeeds/urjanet/datasource/%s.py" % utility_filename
    with open(filename, "w") as outf:
        data = re.sub("_UtilityName_", utility_name, data)
        outf.write(re.sub("_UtilityProvider_", utility_provider, data))
        print("wrote datasource to %s" % (filename))

    # Create a transformer class from a template.
    with open("templates/urjanet_transformer%s.py" % suffix) as inf:
        data = inf.read()
    filename = "../datafeeds/urjanet/transformer/%s.py" % utility_filename
    with open(filename, "w") as outf:
        outf.write(re.sub("_UtilityName_", utility_name, data))
        print("wrote transformer to %s" % (filename))

    print("\nadd to datafeeds/urjanet/transformer/__init__.py\n")
    print("from .%s import %sTransformer" % (utility_filename, utility_name))
    # add import and key to launch.py
    print("\nadd import and key to launch.py\n")
    print(
        "from datafeeds.urjanet.datasource.%s import datafeed as %s"
        % (utility_filename, utility_filename)
    )
    print("\nadd key to scraper_functions in launch.py\n")
    print('"%s-urjanet": %s,' % (utility_id, utility_filename))


def generate_tests(utility_id: str, utility_name: str, utility_filename: str):
    """Generate tests from fixture data and template.

    Read csv with expected values from datafeeds/urjanet/tests/data/utility-id.csv
    Export JSON data from Urjanet data source for each utility_account_id / service_id to
    datafeeds/urjanet/tests/data/utility_id/key.json
    """
    # Create a test class from a template.
    with open("templates/test_urjanet_transformer.py") as inf:
        data = inf.read()
    test_filename = (
        "../datafeeds/urjanet/tests/test_%s_transformer.py" % utility_filename
    )
    with open(test_filename, "w") as outf:
        data = re.sub("_UtilityName_", utility_name, data)
        outf.write(re.sub("_UtilityId_", utility_id, data))
        print("wrote test to %s" % (test_filename))

    # read fixture csv
    keys: Dict[str, Dict[Any]] = {}
    filename = "../datafeeds/urjanet/tests/data/%s.csv" % utility_id
    print("reading fixture data from %s" % filename)
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["service_id"] if row["service_id"] else row["utility_account_id"]
            keys[key] = row

    # dump data for each key (service_id or utility_account_id)
    path = "datafeeds.urjanet.datasource.%s.%sDatasource" % (
        utility_filename,
        utility_name,
    )
    datasource_class = locate(path)
    datasource = datasource_class(utility_id, "account_number")  # placeholders
    datasource.conn = pymysql.connect(
        host=config.URJANET_MYSQL_HOST,
        user=config.URJANET_MYSQL_USER,
        passwd=config.URJANET_MYSQL_PASSWORD,
        db=config.URJANET_MYSQL_DB,
    )
    try:
        os.mkdir("../datafeeds/urjanet/tests/data/%s" % utility_filename)
    except FileExistsError:
        pass
    for key in keys:
        print("loading Urjanet data for %s" % key)
        row = keys[key]
        datasource.account_number = row["utility_account_id"]
        datasource.service_id = row["service_id"]
        data = fetch_data(datasource)
        filename = "../datafeeds/urjanet/tests/data/%s/%s.json" % (
            utility_filename,
            key,
        )
        with open(filename, "w") as f:
            f.write(json.dumps(data, indent=2))
            print("wrote %s" % filename)
    print(
        "\nrun tests:\npython -m unittest datafeeds/urjanet/tests/test_%s_transformer.py"
        % utility_filename
    )


def main():
    parser = argparse.ArgumentParser(description="set up files for outsourcing Urjanet")
    parser.add_argument("op", type=str, help="what to generate: classes or tests")
    parser.add_argument(
        "utility_id", type=str, help="utility code, ie contra-costa-water"
    )
    parser.add_argument(
        "utility_provider",
        type=str,
        help="Urjanet utility code, ie ContraCostaWaterDistrictCA",
    )
    parser.add_argument(
        "--water",
        action="store_const",
        const=True,
        help="if set, use GenericWater base classes",
    )
    args = parser.parse_args()

    utility_name = args.utility_id.title().replace("-", "")
    utility_filename = args.utility_id.replace("-", "_")
    suffix = "_water" if args.water else ""

    if args.op == "classes":
        generate_classes(
            args.utility_id,
            args.utility_provider,
            utility_name,
            utility_filename,
            suffix,
        )
    elif args.op == "tests":
        generate_tests(args.utility_id, utility_name, utility_filename)
    else:
        print("unknown operation %s" % args.op)


if __name__ == "__main__":
    main()
