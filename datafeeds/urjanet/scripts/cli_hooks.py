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

from datafeeds.urjanet.datasource.american import AmericanWaterDatasource
from datafeeds.urjanet.datasource.austin_tx import AustinTXDatasource
from datafeeds.urjanet.datasource.base import CommodityType
from datafeeds.urjanet.datasource.calwater import CalWaterDatasource
from datafeeds.urjanet.datasource.colleyville import ColleyvilleWaterDatasource
from datafeeds.urjanet.datasource.fortworth import FortWorthWaterDatasource
from datafeeds.urjanet.datasource.fostercity import FosterCityWaterDatasource
from datafeeds.urjanet.datasource.heco import HecoDatasource
from datafeeds.urjanet.datasource.irvineranch import IrvineRanchWaterDatasource

from datafeeds.urjanet.datasource.ladwp import LADWPDatasource
from datafeeds.urjanet.datasource.ladwp_water import LosAngelesWaterDatasource
from datafeeds.urjanet.datasource.mountainview import MountainViewDatasource
from datafeeds.urjanet.datasource.nationalgrid import NationalGridDatasource
from datafeeds.urjanet.datasource.pge import PacificGasElectricDatasource
from datafeeds.urjanet.datasource.pse import PseDatasource
from datafeeds.urjanet.datasource.pleasanton import PleasantonDatasource
from datafeeds.urjanet.datasource.sandiego import SanDiegoWaterDatasource
from datafeeds.urjanet.datasource.sdge import SDGEDatasource
from datafeeds.urjanet.datasource.sfpuc import SanFranciscoWaterDatasource
from datafeeds.urjanet.datasource.sjwater import SjWaterDatasource
from datafeeds.urjanet.datasource.southlake import SouthlakeDatasource
from datafeeds.urjanet.datasource.watauga import WataugaDatasource
from datafeeds.urjanet.transformer import (
    UrjanetGridiumTransformer,
    LADWPTransformer,
    LosAngelesWaterTransformer,
    NationalGridTransformer,
    PacificGasElectricTransformer,
    SanFranciscoWaterTransformer,
    FosterCityWaterTransformer,
    GenericWaterTransformer,
    SDGETransformer,
    SouthlakeTransformer,
    WataugaTransformer,
    AmericanTransformer,
    AustinTXTransformer,
    HecoTransformer,
)

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
            RegisteredCliHook.register(
                cli_key, newclass
            )  # here is your register function
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


class NationalGridCli(DatasourceCli):
    __cli_key__ = "nationalgrid"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("said", help="utility_service.service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            NationalGridDatasource(args.account_number, args.said), conn
        )

    def make_transformer(self):
        return NationalGridTransformer()


class PgeCli(DatasourceCli):
    __cli_key__ = "pge"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            PacificGasElectricDatasource(args.account_number, args.service_id), conn
        )

    def make_transformer(self):
        return PacificGasElectricTransformer()


class PseCli(DatasourceCli):
    __cli_key__ = "pse"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("said", help="utility_service.service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            PseDatasource(args.account_number, args.said), conn
        )

    def make_transformer(self):
        return UrjanetGridiumTransformer()


class LadwpWaterCli(DatasourceCli):
    __cli_key__ = "ladwp_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            LosAngelesWaterDatasource(args.account_number, args.service_id), conn
        )

    def make_transformer(self):
        return LosAngelesWaterTransformer()


class LadwpCli(DatasourceCli):
    __cli_key__ = "ladwp"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            LADWPDatasource(args.account_number, args.service_id), conn
        )

    def make_transformer(self):
        return LADWPTransformer()


class SanFranciscoWaterCli(DatasourceCli):
    __cli_key__ = "sfpuc_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):

        return self.setup_datasource(
            SanFranciscoWaterDatasource(args.account_number), conn
        )

    def make_transformer(self):
        return SanFranciscoWaterTransformer()


class FosterCityWaterCli(DatasourceCli):
    __cli_key__ = "fostercity_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            FosterCityWaterDatasource(args.account_number), conn
        )

    def make_transformer(self):
        return FosterCityWaterTransformer()


class ColleyvilleWaterCli(DatasourceCli):
    __cli_key__ = "colleyville_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            ColleyvilleWaterDatasource(args.account_number), conn
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class FortWorthWaterCli(DatasourceCli):
    __cli_key__ = "fortworth_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            FortWorthWaterDatasource(args.account_number), conn
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class SjWaterCli(DatasourceCli):
    __cli_key__ = "sj_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(SjWaterDatasource(args.account_number), conn)

    def make_transformer(self):
        return GenericWaterTransformer()


class SanDiegoWaterCli(DatasourceCli):
    __cli_key__ = "sandiego_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(SanDiegoWaterDatasource(args.account_number), conn)

    def make_transformer(self):
        return GenericWaterTransformer()


class SDGECli(DatasourceCli):
    __cli_key__ = "sdge"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("said", help="utility_service.service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            SDGEDatasource(args.account_number, args.said), conn
        )

    def make_transformer(self):
        return SDGETransformer()


class SouthlakeCli(DatasourceCli):
    __cli_key__ = "southlake"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(SouthlakeDatasource(args.account_number), conn)

    def make_transformer(self):
        return SouthlakeTransformer()


class WataugaCli(DatasourceCli):
    __cli_key__ = "watauga"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(WataugaDatasource(args.account_number), conn)

    def make_transformer(self):
        return WataugaTransformer()


def get_cli_hooks() -> Dict[str, Type[DatasourceCli]]:
    return _cli_hook_registry


class IrvineRanchWaterCli(DatasourceCli):
    __cli_key__ = "irvineranch_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            IrvineRanchWaterDatasource(args.account_number), conn
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class CalWaterCli(DatasourceCli):
    __cli_key__ = "calwater"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(CalWaterDatasource(args.account_number), conn)

    def make_transformer(self):
        return GenericWaterTransformer()


class MountainViewCli(DatasourceCli):
    __cli_key__ = "mountainview"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(MountainViewDatasource(args.account_number), conn)

    def make_transformer(self):
        return GenericWaterTransformer()


class PleasantonCli(DatasourceCli):
    __cli_key__ = "pleasanton"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(PleasantonDatasource(args.account_number), conn)

    def make_transformer(self):
        return GenericWaterTransformer()


class AmericanWaterCli(DatasourceCli):
    __cli_key__ = "american"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(AmericanWaterDatasource(args.account_number), conn)

    def make_transformer(self):
        return AmericanTransformer()


class AustinTXCli(DatasourceCli):
    __cli_key__ = "city-of-austin"  # match utility identifier

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("commodity_type", help="meter table commodity: kw or ccf")
        parser.add_argument("said", help="utility_service.service_id")

    def make_datasource(self, conn, args):
        commodity_type = CommodityType[args.commodity_type]
        return self.setup_datasource(
            AustinTXDatasource(args.account_number, commodity_type, args.said), conn
        )

    def make_transformer(self):
        return AustinTXTransformer()


class HecoCli(DatasourceCli):
    __cli_key__ = "heco"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("said", help="utility_service.service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            HecoDatasource(args.account_number, args.said), conn
        )

    def make_transformer(self):
        return HecoTransformer()
