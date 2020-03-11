import logging
import time
from datetime import date, datetime, timedelta
from typing import Optional, List, Dict

import requests
from dateutil.relativedelta import relativedelta

from datafeeds import config
from datafeeds.common.batch import run_datafeed
from datafeeds.common.exceptions import DataSourceConfigurationError
from datafeeds.common.timeline import Timeline
from datafeeds.common.base import BaseApiScraper
from datafeeds.common.support import Configuration
from datafeeds.common.support import Results
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

logger = None
log = logging.getLogger(__name__)

CERT_PATH = "/tmp/smt_client.cert"
KEY_PATH = "/tmp/smt_client.key"
SMT_ENDPOINT = "https://services.smartmetertexas.net/15minintervalreads/"


class SmartMeterTexasConfiguration(Configuration):
    def __init__(self, esiid):
        super().__init__(scrape_readings=True)  # SMT only provides interval readings.
        self.esiid = esiid  # An ESIID identifies a single meter, similar to Service ID.


class ApiException(Exception):
    pass


class SmartMeterTexasScraper(BaseApiScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "Smart Meter Texas"

    @property
    def esiid(self):
        return self._configuration.esiid

    @staticmethod
    def parse(content: dict) -> Dict[date, List[Optional[float]]]:
        """Convert response JSON to a list of floating point values."""
        daily_data = content.get("energyData", [])

        if not daily_data:
            log.warning("No energy data present in response.")

        results = {}
        for record in daily_data:
            date_label = record.get("DT")
            try:
                day = datetime.strptime(date_label, "%m/%d/%Y").date()
            except (TypeError, ValueError):
                log.error("Failed to process date label: %s", date_label)
                continue

            data_str = record.get("RD", "")

            buffer = []
            for datum_str in data_str.split(","):
                # The API appears to have a bug where empty entries are injected into the list
                # that do not represent null/unknown interval data. We need to drop these, since they appear
                # consistently.
                if datum_str.strip() == "":
                    continue

                try:
                    datum = float(datum_str.replace("-A", "").replace("-E", ""))
                except (ValueError, TypeError):
                    datum = None

                buffer.append(datum)

            if len(buffer) != 96:
                raise ApiException(
                    "Unexpected daily interval data received from SMT. Expected 96, found %s.",
                    len(buffer),
                )

            results[day] = buffer

        return results

    def request_data(self, start: date, end: date) -> Dict[date, List[Optional[float]]]:
        body = {
            "trans_id": "gridium%d" % time.time(),
            "requestorID": config.SMT_API_USERNAME,
            "requesterType": "CSP",
            "requesterAuthenticationID": config.SMT_API_AUTHENTICATION_ID,
            "startDate": start.strftime("%m/%d/%Y"),
            "endDate": end.strftime("%m/%d/%Y"),
            "reportFormat": "JSON",
            "version": "A",
            "readingType": "A",
            "esiid": [self.esiid],
            "SMTTermsandConditions": "Y",
        }

        # Note: Because of SMT's security settings, this request only works if issued inside our production VPC.
        response = requests.post(
            SMT_ENDPOINT,
            cert=(CERT_PATH, KEY_PATH),
            verify=False,  # It would be nice to remove this, but SMT's certificate is faulty.
            auth=(config.SMT_API_USERNAME, config.SMT_API_PASSWORD),
            json=body,
        )

        if response.status_code != requests.codes.ok:
            log.error(
                "API request to Smart Meter Texas failed. Status Code: %s. Text: %s",
                response.status_code,
                response.text,
            )
            return dict()

        try:
            response_body = response.json()
        except ValueError:
            log.error(
                "Response body failed to decode as JSON. Text: %s" % response.text
            )
            return dict()

        return self.parse(response_body)

    def _execute(self) -> Results:
        with open("/tmp/smt_client.cert", "w") as cert_file:
            cert_file.write(config.SMT_CLIENT_CERT)

        with open("/tmp/smt_client.key", "w") as key_file:
            key_file.write(config.SMT_CLIENT_CERT_KEY)

        # No more than 24 months are available on this service.
        start = max(self.start_date, date.today() - relativedelta(months=23))
        end = min(self.end_date, date.today())
        log.info("Final date range: %s - %s" % (start, end))

        timeline = Timeline(self.start_date, self.end_date)

        current_dt = self.start_date
        while current_dt < self.end_date:
            log.info("Requesting data for %s.", current_dt)
            results = self.request_data(current_dt, current_dt)
            for day, use_data in results.items():
                for ii, use_value in enumerate(use_data):
                    timeline.insert(
                        datetime(day.year, day.month, day.day)
                        + timedelta(minutes=15 * ii),
                        use_value * 4 if use_value is not None else None,
                    )
            current_dt += timedelta(days=1)

        return Results(readings=timeline.serialize())


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    """Run the SMT API integration to gather interval data."""
    esiid = (datasource.meta or {}).get("esiid")

    if esiid is None:
        msg = "Missing ESIID for datasource {}, meter {}.".format(
            datasource.oid, meter.oid
        )
        raise DataSourceConfigurationError(msg)

    configuration = SmartMeterTexasConfiguration(esiid)

    return run_datafeed(
        SmartMeterTexasScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
