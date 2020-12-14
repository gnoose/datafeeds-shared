import os
import json
import logging
import re
import hashlib
from typing import Optional

import requests

from datafeeds import config
from datafeeds.common.util.s3 import s3_key_exists, upload_file_to_s3
from datafeeds.models.bill import PartialBillProviderType
from datafeeds.urjanet.model import (
    Charge,
    GridiumBillingPeriod,
    order_json,
    GridiumBillingPeriodCollection,
)
from datafeeds.urjanet.datasource.base import UrjanetDataSource
from datafeeds.urjanet.transformer import UrjanetGridiumTransformer
from datafeeds.common.base import BaseScraper
from datafeeds.common.support import Results, Configuration
from datafeeds.common.typing import (
    AttachmentEntry,
    BillingDatum,
    BillingDatumItemsEntry,
)


log = logging.getLogger(__name__)


def statement_to_s3(source_link, s3_key=None):
    if s3_key:
        if s3_key_exists(config.BILL_PDF_S3_BUCKET, s3_key):
            log.debug("Urjanet statement already uploaded: %s", s3_key)
            return s3_key
    else:  # use id from URL
        m = re.search(r".*?\?id=(.*)&?", source_link)
        if m and m.group(1):
            key = m.group(1)
        else:
            key = hashlib.sha224(source_link.encode("utf-8")).hexdigest()
        for ext in ["pdf", "csv"]:
            # If the s3 key already exists, don't download the PDF statement from urjanet
            s3_filename = "%s.%s" % (key, ext)
            if s3_key_exists(config.BILL_PDF_S3_BUCKET, s3_filename):
                log.debug("Urjanet statement already uploaded: %s", s3_filename)
                return s3_filename

    # This link "source" path appears to be deprecated, but persists
    # in some places in our DB:
    #
    # https://sources.o2.urjanet.net/source?...
    #
    # Let's fix this where we find it.
    bill_link = source_link.replace(
        "https://sources.o2.urjanet.net/source?",
        "https://sources.o2.urjanet.net/sourcewithhttpbasicauth?",
    )

    # Download bill into memory
    # Not ideal, but verify=False prevents:
    # "SSLError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed"
    try:
        log.debug("get bill from urjanet: %s" % bill_link)
        bill = requests.get(
            bill_link,
            auth=(config.URJANET_HTTP_USER, config.URJANET_HTTP_PASSWORD),
            verify=False,
        )

        if bill.status_code != 200 or "content-disposition" not in bill.headers:
            log.info(
                "bill download failed. url=%s, status_code=%d, content-disposition=%s",
                bill_link,
                bill.status_code,
                bill.headers.get("content-disposition"),
            )
            return None
    except Exception as e:
        log.info("bill download failed. url=%s, exception=%s", (bill_link, e))
        return None

    # Parse filename out of 'attachments; filename="some_file.pdf"' and remove ""s
    urja_filename = bill.headers["content-disposition"].split('"')[1]
    # like 'text/csv; charset=utf-8'
    content_type = bill.headers.get("Content-Type", "application/pdf").split(";")[0]
    if s3_key:
        s3_filename = s3_key
    else:
        s3_filename = "%s.%s" % (
            key,
            bill.headers["content-disposition"].split('"')[1].split(".")[1],
        )
    upload_file_to_s3(
        bill.content,
        config.BILL_PDF_S3_BUCKET,
        s3_filename,
        file_display_name=urja_filename,
        content_type=content_type,
    )

    return s3_filename


def _try_parse_float(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def get_charge_kind(charge: Charge):
    result = "other"

    unit = charge.UsageUnit.lower()
    if unit == "kw":
        result = "demand"
    elif unit in ["kwh", "therms", "hcf", "ccf"]:
        result = "use"

    return result


def get_charge_units(charge: Charge):
    unit = charge.UsageUnit.lower()
    if unit in ["kw", "kwh", "therms", "hcf", "ccf"]:
        return unit
    return "other"


def make_line_items(bill: GridiumBillingPeriod):
    charges = bill.line_items
    if not charges:
        return None

    return [
        BillingDatumItemsEntry(
            description=charge.ChargeActualName,
            quantity=_try_parse_float(charge.ChargeUnitsUsed),
            rate=_try_parse_float(charge.ChargeRatePerUnit),
            total=_try_parse_float(charge.ChargeAmount),
            kind=get_charge_kind(charge),
            unit=get_charge_units(charge),
        )
        for charge in charges
    ]


def make_attachments(
    bill: GridiumBillingPeriod,
    utility: str,
    account_id: str,
    gen_utility: Optional[str] = None,
    gen_utility_account_id: Optional[str] = None,
):
    if not config.enabled("S3_BILL_UPLOAD"):
        return None

    source_urls = bill.source_urls
    if not source_urls:
        return None

    s3_keys = [statement_to_s3(url) for url in source_urls]
    attachments = [
        AttachmentEntry(
            key=key,
            kind="bill",
            format="PDF",
            source="urjanet",
            statement=bill.statement.strftime("%Y-%m-%d"),
            utility=utility,
            utility_account_id=account_id,
            gen_utility=gen_utility,
            gen_utility_account_id=gen_utility_account_id,
        )
        for key in s3_keys
        if key is not None
    ]
    if attachments:
        return attachments

    return None


def make_billing_datum(
    bill: GridiumBillingPeriod, utility: str, account_id: str, fetch_attachments=False
) -> BillingDatum:
    return BillingDatum(
        start=bill.start,
        end=bill.end,
        statement=bill.statement,
        cost=_try_parse_float(bill.total_charge),
        used=_try_parse_float(bill.total_usage),
        peak=_try_parse_float(bill.peak_demand),
        items=make_line_items(bill),
        attachments=make_attachments(bill, utility, account_id)
        if fetch_attachments
        else None,
        utility_code=bill.tariff,
        service_id=bill.service_id,
        utility_account_id=bill.utility_account_id,
        utility=bill.utility,
    )


class BaseUrjanetConfiguration(Configuration):
    def __init__(
        self,
        urja_datasource,
        urja_transformer,
        utility_name,
        fetch_attachments,
        partial_type: PartialBillProviderType = None,
    ):
        super().__init__(scrape_bills=True)
        self.urja_datasource = urja_datasource
        self.urja_transformer = urja_transformer
        self.utility_name = utility_name
        self.fetch_attachments = fetch_attachments
        # if Urjanet is a partial billing datasource, set the type of partial it retrieves
        self.partial_type = partial_type
        self.scrape_bills = partial_type is None
        self.scrape_partial_bills = partial_type is not None


class BaseUrjanetScraper(BaseScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Urjanet Scraper: {}".format(self._configuration.utility_name)

    def _execute(self):
        data = self.urja_datasource.load()
        gridium_bills = self.urja_transformer.urja_to_gridium(data)

        if self._configuration.scrape_partial_bills:
            restricted_billing_periods = []
            for period in gridium_bills.periods:
                if (
                    period.start >= self._date_range.start_date
                    and period.end <= self._date_range.end_date
                ):
                    restricted_billing_periods.append(period)
            # Rather than scrape every bill we have, restrict *partial* urjanet scrapers to return data
            # in the date range, so we can scrape the same amount of data for both partial scrapers on a meter.
            gridium_bills = GridiumBillingPeriodCollection(
                periods=restricted_billing_periods
            )

        out_dir = config.WORKING_DIRECTORY
        if out_dir:
            bill_json_file = os.path.join(out_dir, "gridium_bills.json")
            with open(bill_json_file, "w") as f:
                json_data = order_json(gridium_bills.to_json())
                f.write(json.dumps(json_data, indent=4))

        utility = self.urja_datasource.utility
        account_id = self.urja_datasource.account_number
        billing_data_final = [
            make_billing_datum(
                bill, utility, account_id, fetch_attachments=self.fetch_attachments
            )
            for bill in gridium_bills.periods
        ]
        if self._configuration.partial_type == PartialBillProviderType.GENERATION_ONLY:
            return Results(generation_bills=billing_data_final)
        if self._configuration.partial_type == PartialBillProviderType.TND_ONLY:
            return Results(tnd_bills=billing_data_final)
        return Results(bills=billing_data_final)

    @property
    def urja_datasource(self) -> UrjanetDataSource:
        return self._configuration.urja_datasource

    @property
    def urja_transformer(self) -> UrjanetGridiumTransformer:
        return self._configuration.urja_transformer

    @property
    def utility_name(self) -> str:
        return self._configuration.utility_name

    @property
    def fetch_attachments(self) -> bool:
        return self._configuration.fetch_attachments

    def start(self):
        pass

    def stop(self):
        pass
