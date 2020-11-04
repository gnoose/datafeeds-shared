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
from datafeeds.urjanet.datasource.constellation import ConstellationDatasource
from datafeeds.urjanet.datasource.directenergy import DirectEnergyDatasource
from datafeeds.urjanet.datasource.fortworth import FortWorthWaterDatasource
from datafeeds.urjanet.datasource.fostercity import FosterCityWaterDatasource
from datafeeds.urjanet.datasource.fpl import FPLDatasource
from datafeeds.urjanet.datasource.heco import HecoDatasource
from datafeeds.urjanet.datasource.generic_water import GenericWaterDatasource
from datafeeds.urjanet.datasource.irvineranch import IrvineRanchWaterDatasource

from datafeeds.urjanet.datasource.ladwp import LADWPDatasource
from datafeeds.urjanet.datasource.ladwp_water import LosAngelesWaterDatasource
from datafeeds.urjanet.datasource.mountainview import MountainViewDatasource
from datafeeds.urjanet.datasource.nationalgrid import NationalGridDatasource
from datafeeds.urjanet.datasource.nve import NVEnergyDatasource
from datafeeds.urjanet.datasource.pge import PacificGasElectricDatasource
from datafeeds.urjanet.datasource.pge_generation import PacificGasElectricXMLDatasource
from datafeeds.urjanet.datasource.pse import PseDatasource
from datafeeds.urjanet.datasource.pleasanton import PleasantonDatasource
from datafeeds.urjanet.datasource.sandiego import SanDiegoWaterDatasource
from datafeeds.urjanet.datasource.clean_power_alliance import (
    SCECleanPowerAllianceDatasource,
)
from datafeeds.urjanet.datasource.sdge import SDGEDatasource
from datafeeds.urjanet.datasource.sfpuc import SanFranciscoWaterDatasource
from datafeeds.urjanet.datasource.sjwater import SjWaterDatasource
from datafeeds.urjanet.datasource.southlake import SouthlakeDatasource
from datafeeds.urjanet.datasource.tricounty import TriCountyDatasource
from datafeeds.urjanet.datasource.watauga import WataugaDatasource
from datafeeds.urjanet.transformer import (
    UrjanetGridiumTransformer,
    ConstellationTransformer,
    LADWPTransformer,
    LosAngelesWaterTransformer,
    NationalGridTransformer,
    NVEnergyTransformer,
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
    TriCountyTransformer,
    PacificGasElectricUrjaXMLTransformer,
)
from datafeeds.urjanet.transformer.directenergy import DirectEnergyTransformer
from datafeeds.urjanet.transformer.fortworth import FortWorthWaterTransformer

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

    def utility(self):
        return "utility:%s" % self.__cli_key__


class NVEnergyCli(DatasourceCli):
    __cli_key__ = "nve"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("said", help="utility_service.service_id")
        parser.add_argument(
            "meter_number", help="snapmeter_meter_data_source.meta.nveMeterNumber"
        )

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            NVEnergyDatasource(
                self.utility(), args.account_number, args.said, args.meter_number
            ),
            conn,
        )

    def make_transformer(self):
        return NVEnergyTransformer()


class GenericWaterCli(DatasourceCli):
    __cli_key__ = "generic_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument(
            "utility_provider", help="snapmeter_meter_data_source.meta.utility_provider"
        )
        parser.add_argument(
            "conversion_factor",
            help="snapmeter_meter_data_source.meta.conversion_factor",
        )

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            GenericWaterDatasource(
                self.utility(),
                args.utility_provider,
                args.account_number,
                args.conversion_factor,
            ),
            conn,
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class SCECleanPowerAllianceCli(DatasourceCli):
    __cli_key__ = "clean_power_alliance"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("gen_utility", help="utility_service.gen_utility")
        parser.add_argument("gen_said", help="utility_service.gen_service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            SCECleanPowerAllianceDatasource(
                self.utility(),
                args.account_number,
                args.gen_utility,
                args.account_number,
                gen_said=args.gen_said,
            ),
            conn,
        )

    def make_transformer(self):
        return UrjanetGridiumTransformer()


class TriCountyCli(DatasourceCli):
    __cli_key__ = "tricounty"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("said", help="utility_service.service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            TriCountyDatasource(self.utility(), args.account_number, args.said), conn,
        )

    def make_transformer(self):
        return TriCountyTransformer()


class FPLCli(DatasourceCli):
    __cli_key__ = "fpl"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("said", help="utility_service.service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            FPLDatasource(self.utility(), args.account_number, args.said), conn
        )

    def make_transformer(self):
        return UrjanetGridiumTransformer()


class NationalGridCli(DatasourceCli):
    __cli_key__ = "nationalgrid"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("said", help="utility_service.service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            NationalGridDatasource(self.utility(), args.account_number, args.said),
            conn,
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
            PacificGasElectricDatasource(
                self.utility(),
                args.account_number,
                args.service_id,
                "utility:pge",
                args.account_number,
            ),
            conn,
        )

    def make_transformer(self):
        return PacificGasElectricTransformer()


class PgeGenerationCli(DatasourceCli):
    __cli_key__ = "pge_generation"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("service_id")

    def make_datasource(self, conn, args):

        return self.setup_datasource(
            PacificGasElectricXMLDatasource(
                self.utility(),
                args.account_number,
                args.service_id,
                self.utility(),
                args.account_number,
            ),
            conn,
        )

    def make_transformer(self):
        return PacificGasElectricUrjaXMLTransformer()


class DirectEnergy(DatasourceCli):
    __cli_key__ = "directenergy"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            DirectEnergyDatasource(
                self.utility(), args.account_number, args.service_id
            ),
            conn,
        )

    def make_transformer(self):
        return DirectEnergyTransformer()


class PseCli(DatasourceCli):
    __cli_key__ = "pse"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("said", help="utility_service.service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            PseDatasource(self.utility(), args.account_number, args.said), conn
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
            LosAngelesWaterDatasource(
                self.utility(), args.account_number, args.service_id
            ),
            conn,
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
            LADWPDatasource(self.utility(), args.account_number, args.service_id), conn
        )

    def make_transformer(self):
        return LADWPTransformer()


class SanFranciscoWaterCli(DatasourceCli):
    __cli_key__ = "sfpuc_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):

        return self.setup_datasource(
            SanFranciscoWaterDatasource(self.utility(), args.account_number), conn
        )

    def make_transformer(self):
        return SanFranciscoWaterTransformer()


class FosterCityWaterCli(DatasourceCli):
    __cli_key__ = "fostercity_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            FosterCityWaterDatasource(self.utility(), args.account_number), conn,
        )

    def make_transformer(self):
        return FosterCityWaterTransformer()


class ColleyvilleWaterCli(DatasourceCli):
    __cli_key__ = "colleyville_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            ColleyvilleWaterDatasource(self.utility(), args.account_number), conn,
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class Constellation(DatasourceCli):
    __cli_key__ = "constellation"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            self.utility(), ConstellationDatasource(args.account_number), conn
        )

    def make_transformer(self):
        return ConstellationTransformer()


class FortWorthWaterCli(DatasourceCli):
    __cli_key__ = "fortworth_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            FortWorthWaterDatasource(self.utility(), args.account_number), conn,
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class SjWaterCli(DatasourceCli):
    __cli_key__ = "sj_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            SjWaterDatasource(self.utility(), args.account_number), conn
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class SanDiegoWaterCli(DatasourceCli):
    __cli_key__ = "sandiego_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            SanDiegoWaterDatasource(self.utility(), args.account_number), conn
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class SDGECli(DatasourceCli):
    __cli_key__ = "sdge"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")
        parser.add_argument("said", help="utility_service.service_id")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            SDGEDatasource(self.utility(), args.account_number, args.said), conn
        )

    def make_transformer(self):
        return SDGETransformer()


class SouthlakeCli(DatasourceCli):
    __cli_key__ = "southlake"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            self.utility(),
            SouthlakeDatasource(self.utility(), args.account_number),
            conn,
        )

    def make_transformer(self):
        return SouthlakeTransformer()


class WataugaCli(DatasourceCli):
    __cli_key__ = "watauga"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            self.utility(), WataugaDatasource(self.utility(), args.account_number), conn
        )

    def make_transformer(self):
        return WataugaTransformer()


class IrvineRanchWaterCli(DatasourceCli):
    __cli_key__ = "irvineranch_water"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            IrvineRanchWaterDatasource(self.utility(), args.account_number), conn,
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class CalWaterCli(DatasourceCli):
    __cli_key__ = "calwater"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            CalWaterDatasource(self.utility(), args.account_number), conn
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class MountainViewCli(DatasourceCli):
    __cli_key__ = "mountainview"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            MountainViewDatasource(self.utility(), args.account_number), conn,
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class PleasantonCli(DatasourceCli):
    __cli_key__ = "pleasanton"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            PleasantonDatasource(self.utility(), args.account_number), conn,
        )

    def make_transformer(self):
        return GenericWaterTransformer()


class AmericanWaterCli(DatasourceCli):
    __cli_key__ = "american"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            AmericanWaterDatasource(self.utility(), args.account_number), conn
        )

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
            AustinTXDatasource(
                self.utility(), args.account_number, commodity_type, args.said
            ),
            conn,
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
            HecoDatasource(self.utility(), args.account_number, args.said), conn
        )

    def make_transformer(self):
        return HecoTransformer()


class ForthWorthCli(DatasourceCli):
    __cli_key__ = "fort-worth"

    def add_datasource_args(self, parser):
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        return self.setup_datasource(
            FortWorthWaterDatasource(self.utility(), args.account_number), conn
        )

    def make_transformer(self):
        return FortWorthWaterTransformer()


def get_cli_hooks() -> Dict[str, Type[DatasourceCli]]:
    return _cli_hook_registry
