"""This class defines a simple interface for Urjanet datasources that wish to be exposed via the command line.

There are two basic operations supported for Urjanet datasources:
  (1) Loading data from a MySQL database into a JSON format, accomplished on the command line by `dump_urja_json.py`.
      This script generally requires some number of utility-specific arguments to inform which data should be extracted
      from the source database.
  (2) Transforming JSON from (1) into billing periods, accomplished on the command line by `transform_urja_json.py`.
      At this time, this script doesn't require customer arguments per utility.

To expose an Urjanet datasource on the command line, a class with the following format, referred to here as a
"CLI Hook", can be defined:

class <ClassName>(DatasourceCli):
    __cli_key__ = "<unique_key_name>"

    def add_datasource_args(self, parser):
        parser.add_argument("<arg1>")
        parser.add_argument("<arg2>")
        parser.add_argument("...")

    def make_datasource(self, args):
        return setup_datasource(<UrjaDatasource>(conn, args.arg1, args.arg2, ...), conn)

    def make_transformer(self):
        return <UrjaTransformer>()

This class will be automatically registered in the _cli_hook_registry dictionary, which is used by the aforementioned
scripts. Specifically the `dump_urja_json.py` script will add the arguments specified in `add_datasource_args` to a
subparser keyed by `__cli_key__`. The `transform_urja_json.py` script will also add a subparser named according to this
attribute, but currently does not add any additional arguments.
"""

from typing import Dict, Type

import datafeeds.urjanet.datasource as urja_datasource
import datafeeds.urjanet.transformer as urja_transformer

_cli_hook_registry = {}


class RegisteredCliHook(type):
    """This is a metaclass that automatically registers CLI Hooks """

    @staticmethod
    def register(key, cls):
        if key in _cli_hook_registry:
            raise ValueError("A CLI Hook with key '{}' already exists!".format(key))
        _cli_hook_registry[key] = cls

    def __new__(mcs, clsname, bases, attrs):
        newclass = super(RegisteredCliHook, mcs).__new__(mcs, clsname, bases, attrs)
        cli_key = attrs.get("__cli_key__")
        if cli_key:
            RegisteredCliHook.register(cli_key, newclass)  # here is your register function
        return newclass


class DatasourceCli(metaclass=RegisteredCliHook):
    __cli_key__ = None

    def add_subparser(self, subparsers):
        sub = subparsers.add_parser(self.__cli_key__)
        self.add_datasource_args(sub)
        sub.set_defaults(datasource_cli=self)

    def add_datasource_args(self, parser):
        pass

    def make_datasource(self, args):
        return None

    def setup_datasource(self, datasource, conn):
        datasource.conn = conn
        return datasource

    def make_transformer(self):
        return None


class PacgeCli(DatasourceCli):
    __cli_key__ = "pacge"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.PacificGasElectricDataSource(args.account_number, args.service_id),
            conn)

    def make_transformer(self):
        return urja_transformer.PacGeGridiumTransfomer()


class LadwpWaterCli(DatasourceCli):
    __cli_key__ = "ladwp_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.LadwpWaterDatasource(args.account_number, args.service_id),
            conn)

    def make_transformer(self):
        return urja_transformer.LadwpWaterTransformer()


class SfpucWaterCli(DatasourceCli):
    __cli_key__ = "sfpuc_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.SfpucWaterDatasource(args.account_number),
            conn)

    def make_transformer(self):
        return urja_transformer.SfpucWaterTransformer()


class FosterCityWaterCli(DatasourceCli):
    __cli_key__ = "foster_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.FosterCityWaterDatasource(args.account_number),
            conn)

    def make_transformer(self):
        return urja_transformer.FosterCityTransformer()


class ColleyvilleWaterCli(DatasourceCli):
    __cli_key__ = "colleyville_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.ColleyvilleWaterDatasource(args.account_number),
            conn)

    def make_transformer(self):
        return urja_transformer.GenericWaterTransformer()


class FortWorthWaterCli(DatasourceCli):
    __cli_key__ = "fortworth_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.FortWorthWaterDatasource(args.account_number),
            conn)

    def make_transformer(self):
        return urja_transformer.GenericWaterTransformer()


class SjWaterCli(DatasourceCli):
    __cli_key__ = "sj_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.SjWaterDatasource(args.account_number),
            conn)

    def make_transformer(self):
        return urja_transformer.GenericWaterTransformer()


class SanDiegoWaterCli(DatasourceCli):
    __cli_key__ = "sandiego_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.SanDiegoWaterDatasource(args.account_number),
            conn)

    def make_transformer(self):
        return urja_transformer.GenericWaterTransformer()


class SouthlakeCli(DatasourceCli):
    __cli_key__ = "southlake"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.SouthlakeDatasource(args.account_number),
            conn)

    def make_transformer(self):
        return urja_transformer.SouthlakeTransformer()


class WataugaCli(DatasourceCli):
    __cli_key__ = "watauga"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.WataugaDatasource(args.account_number),
            conn)

    def make_transformer(self):
        return urja_transformer.WataugaTransformer()


def get_cli_hooks() -> Dict[str, Type[DatasourceCli]]:
    return _cli_hook_registry


class IrvineRanchWaterCli(DatasourceCli):
    __cli_key__ = "irvineranch_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.IrvineRanchWaterDatasource(args.account_number),
            conn)

    def make_transformer(self):
        return urja_transformer.GenericWaterTransformer()


class CalWaterCli(DatasourceCli):
    __cli_key__ = "calwater"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.CalWaterDatasource(args.account_number),
            conn)

    def make_transformer(self):
        return urja_transformer.GenericWaterTransformer()


class PleasantonCli(DatasourceCli):
    __cli_key__ = "pleasanton"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            urja_datasource.PleasantonDatasource(args.account_number),
            conn)

    def make_transformer(self):
        return urja_transformer.GenericWaterTransformer()
