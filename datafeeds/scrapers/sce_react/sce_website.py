import logging
from typing import List, Optional

from datafeeds.common.base import BaseWebScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Configuration, Results
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)
from datafeeds.scrapers.sce_react.errors import LoginFailedException

from datafeeds.scrapers.sce_react.basic_billing import (
    datafeed as sce_react_basic_billing,
)

from datafeeds.scrapers.sce_react.energymanager_billing import (
    datafeed as sce_react_energymanager_billing,
)
from datafeeds.scrapers.sce_react.energymanager_greenbutton import (
    datafeed as sce_react_energymanager_greenbutton,
)
from datafeeds.scrapers.sce_react.energymanager_interval import (
    datafeed as sce_react_energymanager_interval,
)

log = logging.getLogger(__name__)

scraper_functions = {
    "sce-react-basic-billing": sce_react_basic_billing,
    "sce-react-energymanager-billing": sce_react_energymanager_billing,
    "sce-react-energymanager-greenbutton": sce_react_energymanager_greenbutton,
    "sce-react-energymanager-interval": sce_react_energymanager_interval,
}


class SceWebsiteConfiguration(Configuration):
    def __init__(self, account, meter, datasource, params, task_id):
        super().__init__(metascraper=True)
        self.account = account
        self.meter = meter
        self.datasource = datasource
        self.params = params
        self.task_id = task_id


class SceWebsiteScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "SCE Website"

    @staticmethod
    def _sort_scrapers(scrapers: List[str]):
        # Each list is the order we want the scrapers to run in
        billing_order = ["sce-react-basic-billing", "sce-react-energymanager-billing"]
        interval_order = [
            "sce-react-energymanager-interval",
            "sce-react-energymanager-greenbutton",
        ]
        billing_scrapers = []
        interval_scrapers = []
        for scraper in scrapers or (billing_order + interval_order):
            if scraper in billing_order:
                billing_scrapers.append(scraper)
            if scraper in interval_order:
                interval_scrapers.append(scraper)
        billing_scrapers = billing_scrapers + [
            s for s in billing_order if s not in billing_scrapers
        ]
        interval_scrapers = interval_scrapers + [
            s for s in interval_order if s not in interval_scrapers
        ]
        return billing_scrapers, interval_scrapers

    def _execute(self):
        scrapers = (self._configuration.datasource.meta or {}).get("sources", [])
        log.info("scrapers: %s", scrapers)
        account = self._configuration.account
        log.info("account: %s", account.name)
        meter = self._configuration.meter
        log.info("meter %s:", meter.oid)
        datasource = self._configuration.datasource
        params = self._configuration.params
        task_id = self._configuration.task_id

        results = Results()
        billing_scrapers, interval_scrapers = self._sort_scrapers(scrapers)

        bill_result_list = []
        for scraper_type in billing_scrapers:
            log.info("Starting billing sub-scraper %s", scraper_type)
            df = scraper_functions[scraper_type]
            if not df:
                log.warning("invalid scraper type %s" % scraper_type)
                continue

            try:
                status = df(
                    account, meter, datasource, params, task_id, metascraper=True
                )
                log.info("status of subscraper %s is %s", scraper_type, status)
                bill_result_list.append(status)
                log.info("bill_result_list is %s", bill_result_list)
                log.info("billing scraper %s result=%s", scraper_type, status)
                # keep going since basic_billing does not get PDFs and energymanager_billing does
            except LoginFailedException:
                log.exception("Billing sub-scraper failed to login: %s", scraper_type)
                raise
            except:  # noqa: E722
                log.exception("Billing sub-scraper failed: %s", scraper_type)
            results.bills = Status.best(bill_result_list)
            log.info("results.bills is %s", results.bills)

        for scraper_type in interval_scrapers:
            log.info("Starting interval sub-scraper %s", scraper_type)
            df = scraper_functions[scraper_type]
            try:
                results.readings = df(
                    account, meter, datasource, params, task_id, metascraper=True
                )
                log.info(
                    "interval scraper %s result=%s", scraper_type, results.readings
                )
                if results.readings:
                    break
            except LoginFailedException:
                log.exception("Interval sub-scraper failed to login: %s", scraper_type)
                raise
            except:  # noqa: E722
                log.exception("Interval subscraper failed: %s", scraper_type)

        log.info("Status for bill scrapers: %s", results.bills)
        log.info("Status for interval scrapers: %s", results.readings)
        results.meta_status = Status.best([results.readings, results.bills])
        log.info("meta_status %s", results.meta_status)
        return results


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SceWebsiteConfiguration(
        account=account,
        meter=meter,
        datasource=datasource,
        params=params,
        task_id=task_id,
    )

    return run_datafeed(
        SceWebsiteScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
        disable_login_on_error=True,
    )
