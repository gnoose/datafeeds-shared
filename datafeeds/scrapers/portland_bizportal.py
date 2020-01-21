# pylint: disable=no-member

from collections import namedtuple
import csv
import logging
import re
from tempfile import NamedTemporaryFile
import time
from typing import Optional

from urllib.parse import urljoin, urlencode
from datetime import timedelta, date
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse as parse_time
from pdfminer.pdfparser import PDFSyntaxError
import requests

from datafeeds import config
from datafeeds.common import BillingDatum, Configuration, DateRange, Results
from datafeeds.common.base import BaseApiScraper
from datafeeds.common.batch import run_datafeed

from datafeeds.common.typing import show_bill_summary, Status
from datafeeds.models import SnapmeterAccount, SnapmeterMeterDataSource, Meter
from datafeeds.parsers.pdfparser import pdf_to_str
from datafeeds.common.upload import hash_bill, upload_bill_to_s3

log = logging.getLogger(__name__)

# After this date, CSV bill downloads are generally (but not necessarily) available.
CSV_START_DATE = date(2018, 4, 15)

API_HOST = "bizapi.portlandgeneral.com"
API_HOST_PROTOCOL = "https://%s" % API_HOST


"""
Currently, a single record of bill metadata takes this form in the HTTP response.

        {
            "AccountNumber": "5454780000",
            "BillingId": "545006900156",
            "DownloadBillUrl": "<url>
            "TotalKwh": 93507,
            "AmountDue": 8498.11,
            "DueDate": "2017-06-16T00:00:00",
            "BillDate": "2017-05-31T00:00:00",
            "Details": [
                {
                    "Amount": 8498.11,
                    "Kwh": 93507,
                    "ServiceAddress": "some address"
                }
            ]
        },

Each of these gets represented as a namedtuple.
"""
BillMetaData = namedtuple(
    "BillMetaData",
    ["AccountNumber", "BillingId", "DownloadBillUrl", "TotalKwh", "AmountDue"],
)


class WebsiteDownException(Exception):
    pass


class LoginException(Exception):
    pass


class NoRelevantBillsException(Exception):
    pass


def extract_bill_period(pdf_filename):
    """Convert the PDF to a string so we can determine the dates this bill covers."""
    try:
        text = pdf_to_str(pdf_filename)
    except PDFSyntaxError:
        log.exception("Downloaded bill file failed to parse as a PDF.")
        return None, None

    pattern = r"Service Period\n(\d+/\d+/\d+)\n(\d+/\d+/\d+)"
    match = re.search(pattern, text)

    if match:
        period_a = parse_time(match.group(1))
        period_b = parse_time(match.group(2))
        return min(period_a, period_b), max(period_a, period_b)

    return None, None


class Session:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.session = requests.Session()

    def login(self):
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-US,en;q=0.9",
                "ADRUM": "isAjax:true",
                "Connection": "keep-alive",
                "Host": API_HOST,
                "Origin": API_HOST_PROTOCOL,
                "Referer": urljoin(API_HOST_PROTOCOL, "/UserAccount"),
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4)"
                + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
            }
        )

        body = {
            "AccountGroups": None,
            "InvalidCredentials": False,
            "Password": self.password,
            "RememberMe": False,
            "ReturnUrl": "",
            "UserName": self.username,
        }

        url = urljoin(API_HOST_PROTOCOL, "/api/UserAccount/SignIn")
        log.info("Logging in.")
        response = self.session.post(
            url, json=body, headers={"Content-Type": "application/json"}
        )
        log.info(
            "\tPOST %s: %d (%f sec)"
            % (url, response.status_code, response.elapsed.total_seconds())
        )

        if response.status_code != requests.codes.ok:
            raise LoginException("Failed to log in as %s." % self.username)

        resp_body = response.json()
        if "SecurityToken" not in resp_body:
            raise LoginException(
                "Did not find expected security token in API response."
            )

        self.session.headers["Authorization"] = "Bearer " + resp_body["SecurityToken"]

        return response

    def logout(self):
        """POST a logout message"""

        log.info("Logging out.")
        url = urljoin(API_HOST_PROTOCOL, "/api/UserAccount/SignOut")
        response = self.session.post(url)
        log.info(
            "\tPOST %s: %d (%f sec)"
            % (url, response.status_code, response.elapsed.total_seconds())
        )
        return response

    def _request_bill_metadata(self, start_dt, end_dt, group_name):
        """GET bill metadata JSON from the Bizportal backend."""

        params = {
            "StartDate": start_dt.strftime("%m/%d/%Y"),
            "EndDate": end_dt.strftime("%m/%d/%Y"),
            "SortedByDueDate": "true",
            "EncryptedPersonId": "",
            "IsCustomGroup": "true",
            "CustomGroupName": group_name,
        }

        details_url = API_HOST_PROTOCOL + "/api/BillingAndPaymentHistory/Details"
        response = self.session.get(
            details_url, headers={"Accept": "application/json"}, params=params
        )
        log.info(
            "\tGET %s: %d (%f sec)"
            % (details_url, response.status_code, response.elapsed.total_seconds())
        )

        if response.status_code != requests.codes.ok:
            log.info("Request for bill metadata failed.")
            return None

        try:
            return response.json()
        except ValueError as ve:
            log.info(
                "Request for bill metadata succeeded, but response did not parse as JSON."
            )
            log.info("\t%s" % ve)
            return None

    @staticmethod
    def _parse_metadata_response(record):
        if not isinstance(record.get("BillingSummaries"), list):
            log.error("Bizportal server returned a record with an unexpected format.")
            return []

        summaries = []

        for elt in record.get("BillingSummaries"):
            try:
                summaries.append(
                    BillMetaData(
                        elt["AccountNumber"],
                        elt["BillingId"],
                        elt["DownloadBillUrl"],
                        elt["TotalKwh"],
                        elt["AmountDue"],
                    )
                )
            except KeyError:
                log.error("Encountered a BillSummary with invalid format.")

        return summaries

    def acquire_bill_metadata(self, start_dt, end_dt, group_name):
        """Acquire a list of all bill metadata in the input date range."""

        date_range = DateRange(start_dt, end_dt)

        metadata = []
        for interval in date_range.split_iter(relativedelta(months=5)):
            result = self._request_bill_metadata(
                interval.start_date, interval.end_date, group_name
            )
            metadata += self._parse_metadata_response(result)

        return metadata

    def download_bill(self, bill_url, file_handle):
        log.info("Requesting bill.")
        try:
            resp = self.session.get(bill_url, stream=True)

            log.info(
                "\tGET %s: %d (%f sec)"
                % (bill_url, resp.status_code, resp.elapsed.total_seconds())
            )

            resp.raise_for_status()

            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    file_handle.write(chunk)
                    file_handle.flush()

            return file_handle
        except requests.exceptions.HTTPError:
            log.info("Error requesting bill for download: %s" % bill_url)
            file_handle.close()
            return None

    def _download_csv(self, account_group, start_date, end_date, tries=1):
        """GET the billing data in CSV format, write it to a temporary file."""

        params = {
            "StartDate": start_date.strftime("%m/%d/%Y"),
            "EndDate": end_date.strftime("%m/%d/%Y"),
            "SortedByDueDate": False,
            "IsCustomGroup": True,
            "CustomGroupName": account_group,
            "ReportType": 1,
            "DelimiterType": 1,
        }

        param_str = urlencode(params)

        path = "/api/BillingAndPaymentHistory/Download"
        url = urljoin(API_HOST_PROTOCOL, path)

        output = None
        attempt = 0
        while attempt < tries:
            log.info(
                "Attempting CSV fetch for range %s - %s ..." % (start_date, end_date)
            )
            attempt += 1
            try:
                response = self.session.get(url, params=param_str, stream=True)

                if response.status_code == requests.codes.ok:
                    output = NamedTemporaryFile()
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:  # filter out keep-alive new chunks
                            output.write(chunk)
                log.info(
                    "\tGET %s: %d (%f sec)"
                    % (
                        response.url,
                        response.status_code,
                        response.elapsed.total_seconds(),
                    )
                )
                if output:
                    output.flush()
                    log.info("Download succeeded.")
                    break

                log.info("Download failed. Retrying in 10 seconds...")
                time.sleep(10)
            except ConnectionError:
                log.info("Connection to Portland GE failed, aborting download.")

        return output

    def download_bill_csvs(self, account_group, start_date, end_date):
        """GET the billing data in CSVs, accounting for API limits."""

        # CSV download fails for start dates prior to 4/15.
        start_date = max(start_date, CSV_START_DATE)

        date_range = DateRange(start_date, end_date)
        step = relativedelta(months=5)

        files = []
        for window in date_range.split_iter(step):
            file_handle = self._download_csv(
                account_group, window.start_date, window.end_date
            )
            if file_handle:
                files.append(file_handle)

        return files


class CsvBillParser:
    @staticmethod
    def _parse_bill_lines(csv_reader, service_id):
        # This utility seems to be inconsistent about whether the AB is a
        # prefix or a suffix (they identify the same entity).
        if service_id.startswith("AB") or service_id.endswith("AB"):
            raw_service_id = service_id.replace("AB", "")
            valid_service_ids = {
                raw_service_id,
                raw_service_id + "AB",
                "AB" + raw_service_id,
            }
        else:
            valid_service_ids = [service_id]

        log.info(
            "Seeking Service ID %s in the following forms: %s",
            service_id,
            valid_service_ids,
        )

        row_count = 0

        def _format_number(n):
            return n.replace("$", "").replace(",", "")

        bills = []
        for line in csv_reader:
            row_count += 1

            # CSV cells sometimes contain whitespace characters like \t.
            meter_number = line["Meter Number"].strip()

            if meter_number in valid_service_ids:
                read_date = parse_time(line["Meter Read Date"]).date()
                prev_read_date = parse_time(line["Previous Read Date"]).date()
                end_date = read_date - timedelta(days=1)

                bill = BillingDatum(
                    start=prev_read_date,
                    end=end_date,
                    cost=_format_number(line["Current Charges"]),
                    used=_format_number(line["kWh"]),
                    peak=max(
                        _format_number(line["Demand"]),
                        _format_number(line["On Peak Demand (kW)"]),
                        _format_number(line["Off Peak Demand (kW)"]),
                    ),
                    items=[],
                    attachments=None,
                )

                bills.append(bill)

        log.info("Processed %d CSV rows." % row_count)
        return bills

    @staticmethod
    def parse(filehandle, service_id):
        log.info("Parsing file %s", filehandle.name)

        with open(filehandle.name, "r") as f:
            reader = csv.DictReader(f)
            return CsvBillParser._parse_bill_lines(reader, service_id)


def _overlap(a, b):
    c_start = max(a.start, b.start)
    c_end = min(a.end, b.end)
    return max(c_end - c_start, timedelta())


def _adjust_bill_dates(bills):
    """Ensure that the input list of bills is sorted by date and no two bills have overlapping dates."""
    bills.sort(key=lambda x: x.start)

    final_bills = []
    for b in bills:
        for other in final_bills:
            if _overlap(b, other) > timedelta() or b.start == other.end:
                b = b._replace(start=max(b.start, other.end + timedelta(days=1)))
        final_bills.append(b)

    return final_bills


"""
Bill Unification Rules:
0. PDF bills are a superset of CSV bills, due to how the bizportal site works.
1. If CSV bill data is available, prefer that data but use attachments from the corresponding PDF.
2. Bill time intervals must not overlap.
3. We form the final timeline of bills by "zipper merging" two ordered lists of non-overlapping bills.
"""


def _unify_bill_history(pdf_bills, csv_bills):
    pdf_bills = _adjust_bill_dates(pdf_bills)
    final_bills = []

    for pb in pdf_bills:
        merged_bill = False

        for cb in csv_bills:
            ol = _overlap(cb, pb)
            if ol > timedelta(days=20):
                # It's likely these are the same bill, so merge them.
                final_bills.append(
                    cb._replace(start=pb.start, end=pb.end, attachments=pb.attachments)
                )
                csv_bills.remove(cb)
                merged_bill = True
                break

        if not merged_bill:
            final_bills.append(pb)

    return final_bills


class PortlandBizportalConfiguration(Configuration):
    def __init__(self, account_group, bizportal_account_number, service_id):
        super().__init__(scrape_bills=True)
        self.account_group = account_group

        # We need to guarantee that this ID is a string; it might have a prefix of zeros.
        self.bizportal_account_number = str(bizportal_account_number)
        self.service_id = service_id


class Scraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Portland Bizportal Scraper"

    @property
    def account_group(self):
        return self._configuration.account_group

    @property
    def bizportal_account_number(self):
        return str(self._configuration.bizportal_account_number)

    @property
    def service_id(self):
        return self._configuration.service_id

    def _pdf_bill_download(self):
        """Download bills available via PDF --- these will not have demand data."""
        bills = []
        sess = Session(self.username, self.password)

        sess.login()
        metadata = sess.acquire_bill_metadata(
            self.start_date, self.end_date, self.account_group
        )
        orig_metadata_count = len(metadata)
        log.info("Recovered %d bill metadata records.", orig_metadata_count)
        log.info(
            "Found these distinct account numbers. %s",
            set(md.AccountNumber for md in metadata),
        )

        metadata = [
            md for md in metadata if md.AccountNumber == self.bizportal_account_number
        ]
        log.info("Found %d relevant bill metadata records.", len(metadata))

        for md in metadata:
            with NamedTemporaryFile(mode="wb") as pdf_handle:
                sess.download_bill(API_HOST_PROTOCOL + md.DownloadBillUrl, pdf_handle)

                if not pdf_handle:
                    log.info("No PDF available for bill metadata %s. Skipping." % md)
                    continue

                period_start, period_end = extract_bill_period(pdf_handle.name)

                if not period_start or not period_end:
                    log.info(
                        "Could not determine bill period for metadata %s. Skipping"
                        % str(md)
                    )
                    continue

                bill_attachment = None

                if config.enabled("S3_BILL_UPLOAD"):
                    with open(pdf_handle.name, "rb") as f:
                        key = hash_bill(
                            self.service_id,
                            period_start.date(),
                            period_end.date(),
                            md.AmountDue,
                            0,
                            md.TotalKwh,
                        )
                        bill_attachment = upload_bill_to_s3(f, key)

                bills.append(
                    BillingDatum(
                        start=period_start.date(),
                        end=period_end.date(),
                        cost=md.AmountDue,
                        used=md.TotalKwh,
                        peak=0,
                        attachments=[bill_attachment] if bill_attachment else None,
                        items=[],  # No line items
                    )
                )

        sess.logout()
        bills.sort(key=lambda x: x.start)

        if bills:
            show_bill_summary(bills, "Bills Obtained via PDF Download Menu")
        else:
            msg = (
                "Processed %s bill metadata records but did not locate a relevant bill."
            )
            raise NoRelevantBillsException(msg % orig_metadata_count)

        return bills

    def _csv_bill_download(self):
        sess = Session(self.username, self.password)

        sess.login()
        files = sess.download_bill_csvs(
            self.account_group, self.start_date, self.end_date
        )

        bills = [b for f in files for b in CsvBillParser.parse(f, self.service_id)]
        sess.logout()

        bills.sort(key=lambda x: x.start)

        show_bill_summary(bills, title="Bills Obtained via CSV Download")
        return bills

    def _execute(self):
        if self.end_date - self.start_date < timedelta(days=90):
            log.info("Widening bill window to increase odds of finding bill data.")
            self.start_date = self.end_date - timedelta(days=90)

        log.info("Final date range: %s - %s", self.start_date, self.end_date)

        log.info("=" * 80)
        log.info("Obtaining bills from PDF-download section of Bizportal site.")
        log.info("=" * 80)
        pdf_bills = self._pdf_bill_download()

        log.info("=" * 80)
        log.info("Obtaining bills from CSV-download section of Bizportal site.")
        log.info("=" * 80)
        csv_bills = self._csv_bill_download()

        bills = _unify_bill_history(pdf_bills, csv_bills)
        return Results(bills=bills)


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: SnapmeterMeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = PortlandBizportalConfiguration(
        account_group=datasource.meta.get("accountGroup"),
        bizportal_account_number=datasource.meta.get("bizportalAccountNumber"),
        service_id=meter.service_id,
    )

    return run_datafeed(
        Scraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
