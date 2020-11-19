import itertools
from typing import Optional, List

from datafeeds import db
from datafeeds.common.batch import run_urjanet_datafeed
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.models.bill import PartialBillProviderType
from datafeeds.models.utility_service import (
    UtilityService,
    UtilityServiceSnapshot,
)
from datafeeds.urjanet.datasource.pge import PacificGasElectricDatasource
from datafeeds.urjanet.model import Account, Meter, Charge, Usage
from datafeeds.urjanet.transformer import PacificGasElectricUrjaXMLTransformer
from datafeeds.urjanet.datasource.pymysql_adapter import UrjanetPyMySqlDataSource
import logging


log = logging.getLogger(__name__)


def _create_placeholders(item_list):
    return ",".join(["%s"] * len(item_list))


def _remove_check_digit(account_id: str) -> str:
    """Strip the check digit if it exists
    Ex. 12134241-2 becomes 12134241
    """
    return account_id.split("-", 1)[0]


class PacificGasElectricXMLDatasource(PacificGasElectricDatasource):
    """Initialize a PacG&E XML datasource, for a given meter.

    This "partial billing" datasource scrapes partial bills from data extracted from
    Urjanet's XML feed, as opposed to our Urjanet SQL delivery.  This data is structured differently so we are
    able to more accurately detect third party charges associated with PG&E service ids.

    To reiterate: this datasource extracts a single stream of data: just generation charges, for example.

    Args:
        utility: PG&E
        account_number: A PG&E account number (our meter.utility_service.utility_account_id)
        said: Service account ID for the meter (meter.utility_service.service_id)
        gen_utility: generation utility
        gen_account_number: A PG&E generation account number, here, same as account number
        utility_service: Our utility service record.
    """

    def __init__(
        self,
        utility: str,
        account_number: str,
        said: str,
        gen_utility: str,
        gen_account_number: str,
        utility_service: Optional[UtilityService] = None,
    ):
        super().__init__(utility, account_number, said, gen_utility, gen_account_number)
        self.meter_id = said
        self.account_number = self.normalize_account_number(account_number)
        self.utility_account_number = account_number
        # Setting utility service so we can examine snapshots associated with the service.
        self.utility_service = utility_service
        self.service_ids: List[str] = []

    def load_accounts(self) -> List[Account]:
        """
        Load third party urjanet "accounts" based on any utility account id that we have recorded for the service.

        The "xmlaccount" table has a mix of PG&E and third party Utility Providers.  This generation scraper is
        only loading urja accounts that are *not* PG&E providers.
        """
        self.validate()

        # For testing, where you may not have a utility service.
        utility_account_ids = [self.utility_account_number.strip()]
        stripped_historical_ids = []

        if self.utility_service:
            # Fetching historical utility_account_ids and gen_account_ids from snapshot table
            historical_account_ids = [
                account_id[0].strip()
                for account_id in (
                    db.session.query(UtilityServiceSnapshot.utility_account_id)
                    .filter(
                        UtilityServiceSnapshot.service == self.utility_service.oid,
                        UtilityServiceSnapshot.utility_account_id.isnot(None),
                        UtilityServiceSnapshot.utility_account_id != "",
                    )
                    .all()
                )
            ]
            for account_id in historical_account_ids:
                # PG&E utility account ids sometimes have a check digit (-0) at the end, and sometimes don't.
                # We're including a version w/out the check digit, just in case the Urjanet data
                # is missing it.
                stripped_historical_ids.append(_remove_check_digit(account_id))

            historical_gen_account_ids = [
                account_id[0].strip()
                for account_id in (
                    db.session.query(UtilityServiceSnapshot.gen_utility_account_id)
                    .filter(
                        UtilityServiceSnapshot.service == self.utility_service.oid,
                        UtilityServiceSnapshot.gen_utility_account_id.isnot(None),
                        UtilityServiceSnapshot.gen_utility_account_id != "",
                    )
                    .all()
                )
            ]

            for account_id in historical_gen_account_ids:
                stripped_historical_ids.append(_remove_check_digit(account_id))

            # Combining historical ids with values passed into scraper, and the current service config.
            # The history *should* contain all of these items, but we want to cover our bases.
            utility_account_ids = list(
                set(
                    itertools.chain(
                        historical_account_ids,
                        historical_gen_account_ids,
                        stripped_historical_ids,
                        list(
                            filter(
                                None,
                                [
                                    (self.utility_account_number or "").strip(),
                                    (self.account_number or "").strip(),
                                    (
                                        self.utility_service.utility_account_id or ""
                                    ).strip(),
                                    (
                                        self.utility_service.gen_utility_account_id
                                        or ""
                                    ).strip(),
                                ],
                            )
                        ),
                    )
                )
            )

        log.info(
            "Searching across these PG&E account number variations: {}".format(
                [num for num in utility_account_ids]
            )
        )

        # Locate accounts that are associated with the utility-account-numbers that
        # are *not* PG&E, leaving third party.
        query = """
           SELECT *
           FROM xmlaccount
           WHERE
               RawAccountNumber REGEXP %s
               AND UtilityProvider != 'PacGAndE'
        """

        accounts = [
            UrjanetPyMySqlDataSource.parse_account_row(row)
            for row in self.fetch_all(query, "|".join(utility_account_ids))
        ]
        account_pks = [account.PK for account in accounts]
        self.service_ids = self.get_all_service_ids(account_pks)
        log.info(
            "Searching for third party charges across these PG&E service_ids: {}".format(
                [num for num in self.service_ids]
            )
        )
        return accounts

    def get_historical_service_ids(self) -> List[str]:
        """
        Loads every service id and generation service id that we have on record for this utility service.

        Service ids can (and frequently do) change over time, so multiple service ids we'll help us locate a more
        complete history of Urjanet "meters".

        """
        # For testing, where you may not have a utility service.
        service_ids = [self.meter_id.strip()]

        if self.utility_service:
            historical_service_ids = [
                result[0].strip()
                for result in (
                    db.session.query(UtilityServiceSnapshot.service_id)
                    .filter(
                        UtilityServiceSnapshot.service == self.utility_service.oid,
                        UtilityServiceSnapshot.service_id.isnot(None),
                        UtilityServiceSnapshot.service_id != "",
                    )
                    .all()
                )
            ]

            gen_historical_service_ids = [
                result[0].strip()
                for result in (
                    db.session.query(UtilityServiceSnapshot.gen_service_id)
                    .filter(
                        UtilityServiceSnapshot.service == self.utility_service.oid,
                        UtilityServiceSnapshot.gen_service_id.isnot(None),
                        UtilityServiceSnapshot.gen_service_id != "",
                    )
                    .all()
                )
            ]

            # Combining historical SAIDs with values passed into scraper, and the current service config.
            # The history *should* contain all of these items, but we want to cover our bases.
            service_ids = list(
                set(
                    itertools.chain(
                        historical_service_ids,
                        gen_historical_service_ids,
                        list(
                            filter(
                                None,
                                [
                                    (self.meter_id or "").strip(),
                                    (self.utility_service.service_id or "").strip(),
                                    (self.utility_service.gen_service_id or "").strip(),
                                ],
                            )
                        ),
                    )
                )
            )

        return service_ids

    def get_service_address(self, service_ids: List[str]) -> Optional[str]:
        """Return the first service address associated with these PODids"""
        query = """
            SELECT ServiceAddress
            FROM xmlmeter
            WHERE PODid IN ({})
        """.format(
            _create_placeholders(service_ids)
        )
        result = self.fetch_one(query, *service_ids)
        return result.get("ServiceAddress") if result else None

    def get_service_type(self, service_ids: List[str]) -> Optional[str]:
        """Return the first service type associated with these PODids"""
        query = """
             SELECT ServiceType
             FROM xmlmeter
             WHERE PODid IN ({})
        """.format(
            _create_placeholders(service_ids)
        )
        result = self.fetch_one(query, *service_ids)
        return result.get("ServiceType") if result else None

    def get_all_service_ids(self, account_pks: List[int]) -> List[str]:
        """
        Supplements known SAIDs with Third Party ESP Customer Numbers.

        Typically, Third Party Urjanet "Meters" will have the same "ESP Customer Number" as the PG&E SAID, so it is easy
        to locate related charges.  However, sometimes PG&E assigns a different ESP Customer Number.  To locate
        additional ESP Customer Numbers that are related to PG&E SAID's, look for third party meters with charges
        whose ChargeUnitsUsed match PG&E charges in the same billing period at the same address.

        Address alone is not enough to narrow down, as service addresses are not always unique.
        """
        service_ids = self.get_historical_service_ids()

        service_address = self.get_service_address(service_ids)
        service_type = self.get_service_type(service_ids)
        if account_pks and service_address and service_type:
            # Creates a temporary table of T&D charges at the
            # same address, on the same type of meter, associated with PODids we have on record.
            query = """
               CREATE TEMPORARY TABLE tnd_charges AS
               SELECT Charge.ChargeUnitsUsed, Charge.IntervalStart
               FROM xmlaccount Account, xmlmeter Meter, xmlcharge Charge
               WHERE Charge.MeterFK = Meter.PK
                   AND Account.PK = Meter.AccountFK
                   AND Meter.PODid in ({})
                   AND Meter.ServiceAddress = %s
                   AND Meter.ServiceType = %s
                   AND Account.UtilityProvider = 'PacGAndE'
                   AND Charge.ChargeUnitsUsed is not null;
            """.format(
                _create_placeholders(service_ids)
            )

            self.execute(query, *service_ids, service_address.upper(), service_type)

            # Look for missing Third Party PODids at the same address,
            # same service type, and for the same month, that have "ChargeUnitsUsed" that correspond
            # to the T&D charges. For example, a T&D bill may have 80.000000 kWh charged at some rate,
            # and the corresponding third party bill will have 80.000000 kWh charged at a different rate.
            query = """
               SELECT distinct(Meter.PODid)
               FROM xmlmeter Meter, xmlcharge Charge, tnd_charges
               WHERE Meter.PK = Charge.MeterFK
                   AND Meter.AccountFK in ({})
                   AND Meter.ServiceType = %s
                   AND Meter.IntervalStart = tnd_charges.IntervalStart
                   AND Charge.ChargeUnitsUsed = tnd_charges.ChargeUnitsUsed
                   AND UPPER(Meter.ServiceAddress) = %s
               """.format(
                _create_placeholders(account_pks)
            )

            meter_pod_id_results = self.fetch_all(
                query, *account_pks, service_type, service_address
            )
            esp_customer_numbers = [
                result.get("PODid") for result in meter_pod_id_results
            ]

            log.info(
                "Additional ESP Customer Numbers located: {}".format(
                    [num for num in esp_customer_numbers if num not in service_ids]
                )
            )
            # Temp table cleanup
            query = """
               DROP TABLE tnd_charges;
           """
            self.execute(query)

            return list(set(service_ids + esp_customer_numbers))
        return service_ids

    def load_meters(self, account_pk: int) -> List[Meter]:
        """Load meters based on all SAID's we have on record, and any ESP Customer Numbers found.
        """
        query = """
           SELECT *
           FROM xmlmeter
           WHERE
               AccountFK=%s
               AND ServiceType in ('electric', 'natural_gas', 'lighting')
               AND PODid in ({})
        """.format(
            _create_placeholders(self.service_ids)
        )
        result_set = self.fetch_all(query, account_pk, *self.service_ids)
        results = [UrjanetPyMySqlDataSource.parse_meter_row(row) for row in result_set]
        return results

    def load_meter_charges(self, account_pk: int, meter_pk: int) -> List[Charge]:
        """Fetch all charge info for a given meter"""
        query = """
            SELECT *
            FROM xmlcharge
            WHERE AccountFK=%s AND MeterFK=%s
        """
        result_set = self.fetch_all(query, account_pk, meter_pk)
        return [UrjanetPyMySqlDataSource.parse_charge_row(row) for row in result_set]

    def load_meter_usages(self, account_pk: int, meter_pk: int) -> List[Usage]:
        """Fetch all usages for a given meter"""
        query = """
            SELECT *
            FROM xmlusage
            WHERE AccountFK=%s AND MeterFK=%s
        """
        result_set = self.fetch_all(query, account_pk, meter_pk)
        return [UrjanetPyMySqlDataSource.parse_usage_row(row) for row in result_set]

    def load_floating_charges(self, account_pk: int) -> List:
        """Floating charges are charges on a statement that are only attached to an account, not a specific meter.
        With our XML data, we are still scraping these "floating" charges, but they are much more
        likely to be "subtotal" charges, rather than misclassified charges.
        """
        return []


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    utility_service = meter.utility_service
    return run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        PacificGasElectricXMLDatasource(
            utility_service.utility,
            utility_service.utility_account_id,
            utility_service.service_id,
            utility_service.gen_utility,
            utility_service.gen_utility_account_id,
            utility_service,
        ),
        PacificGasElectricUrjaXMLTransformer(),
        task_id=task_id,
        partial_type=PartialBillProviderType.GENERATION_ONLY,
    )
