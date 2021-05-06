import logging
from typing import Optional, List

from datafeeds import db
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Results
from datafeeds.models.bill import PartialBillProviderType
from datafeeds.scrapers.smd_partial_bills.synchronizer import get_service_ids
from datafeeds.urjanet.datasource.pge_generation import PacificGasElectricXMLDatasource

from datafeeds.common.typing import Status, BillPdf
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.urjanet.datasource.pymysql_adapter import create_placeholders
from datafeeds.urjanet.scraper import (
    BaseUrjanetConfiguration,
    BaseUrjanetScraper,
    make_attachments,
)
from datafeeds.urjanet.transformer import PacificGasElectricUrjaXMLTransformer

log = logging.getLogger(__name__)


class PacGAndEPDFAndGenPartialScraper(BaseUrjanetScraper):
    def get_urja_pdfs_for_service(self) -> List[BillPdf]:
        service_ids = get_service_ids(self.urja_datasource.utility_service)  # type: ignore

        log.info(
            "Looking for Urjanet PDFs to attach to existing SMD Bills on service ids: %s",
            service_ids,
        )
        utility_account_id = self.urja_datasource.utility_service.utility_account_id  # type: ignore

        # Order by StatementDate ASC so most recent statement will be added as the first attachment on a bill.
        query = """
           SELECT xmlaccount.SourceLink, xmlaccount.StatementDate, xmlmeter.IntervalStart, xmlmeter.IntervalEnd
           FROM xmlaccount, xmlmeter
           WHERE xmlaccount.PK = xmlmeter.AccountFK
               AND xmlaccount.UtilityProvider = 'PacGAndE'
               AND (RawAccountNumber LIKE %s OR REPLACE(RawAccountNumber, '-', '')=%s)
               AND PODid in ({})
           ORDER BY xmlmeter.IntervalStart DESC, xmlaccount.StatementDate ASC
        """.format(
            create_placeholders(service_ids)
        )
        pge_pdfs: List[BillPdf] = []
        account_number_prefix_regex = "{}%".format(utility_account_id)
        result_set = self.urja_datasource.fetch_all(  # type: ignore
            query, account_number_prefix_regex, utility_account_id, *service_ids
        )

        for row in result_set:
            source_url = row.get("SourceLink")
            statement = row.get("StatementDate") or row.get("IntervalEnd")
            start = row.get("IntervalStart")
            end = row.get("IntervalEnd")

            attachments = make_attachments(
                source_urls=[source_url],
                statement=statement,
                utility="utility:pge",
                account_id=utility_account_id,
                gen_utility=None,
                gen_utility_account_id=None,
            )
            if attachments:
                att = attachments[0]
                pge_pdfs.append(
                    BillPdf(
                        utility_account_id=utility_account_id,
                        gen_utility_account_id="",
                        start=start,
                        end=end,
                        statement=statement,
                        s3_key=att.key,
                    )
                )

        return pge_pdfs

    def _execute(self):
        # Scrape generation partial bills
        generation_billing_data = self.gridium_bills_to_billing_datum()

        log.info("=" * 80)
        gen_utilities = [
            bd.utility for bd in generation_billing_data if bd.utility is not None
        ]
        if gen_utilities:
            # Set gen utility if we can find it.  It's difficult to rely on Urja if the meter is currently
            # on a third party so we will just set this value if we have it.
            gen_utility = gen_utilities[-1]
            self.utility_service.gen_utility = gen_utility
            log.info(
                "Found generation utility %s on service %s.",
                gen_utility,
                self.utility_service.service_id,
            )
        log.info("=" * 80)

        # Look for PDF's to attach to SMD Bills, because SMD doesn't give us PDFs.
        pge_pdfs = self.get_urja_pdfs_for_service()
        return Results(generation_bills=generation_billing_data, pdfs=pge_pdfs)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    """
    pge-urjanet-v3 has three jobs: 1) Scrape any generation partial bills 2) Set generation utility, if we have it.
    3) Look for any PDF's on the service that we can attach to existing SMD Bills or SMD Partial Bills.
    """
    utility_service = meter.utility_service

    urja_datasource = PacificGasElectricXMLDatasource(
        utility_service.utility,
        utility_service.utility_account_id,
        utility_service.service_id,
        utility_service.gen_utility,
        utility_service.gen_utility_account_id,
        utility_service,
    )

    transformer = PacificGasElectricUrjaXMLTransformer()

    conn = db.urjanet_connection()

    try:
        urja_datasource.conn = conn
        scraper_config = BaseUrjanetConfiguration(
            urja_datasource=urja_datasource,
            urja_transformer=transformer,
            utility_name=meter.utility_service.utility,
            fetch_attachments=True,
            partial_type=PartialBillProviderType.GENERATION_ONLY,
        )

        scraper_config.scrape_pdfs = True

        return run_datafeed(
            PacGAndEPDFAndGenPartialScraper,
            account,
            meter,
            datasource,
            params,
            configuration=scraper_config,
            task_id=task_id,
            meter_only=True,  # Upload PDF's found to just this meter, not others in account.
        )
    finally:
        conn.close()
