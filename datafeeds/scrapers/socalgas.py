import logging

from typing import Optional, Tuple, List, Dict, Callable

from datafeeds.common.batch import run_datafeed
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.support import Configuration
from datafeeds.common.typing import Status
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)


IFRAME_ACCOUNT_RGX = re.compile(r'(?<=CustomerId=)[0-9]+')
DOWNLOAD_FILENAME_RGX = re.compile(r'(?<=zip file titled: ).+')

IFRAME_SEL = 'iframe[title="Ways To Save"]'

GREEN_BUTTON_SEL = '#la-links-group #la-greenbutton-view-trigger'
START_INPUT_SEL = '#la-greenbutton-container input#txtStartDate'
END_INPUT_SEL = '#la-greenbutton-container input#txtEndDate'
USERNAME_SEL = '#pt1\\:pli1\\:loginid\\:\\:content'
PASSWORD_SEL = '#pt1\\:pwli\\:pwd\\:\\:content'
MY_BILL_MENU_HEADER_SEL = '#pt1\\\\:pt_dc2\\\\:desktopNav > ul:nth-child(1) > li:nth-child(2)'
BILL_HISTORY_LINK_SEL = '#pt1\\:pt_dc2\\:i1\\:1\\:i2\\:11\\:navChild'
BILL_HISTORY_TABLE_SEL = '#pt1\\:pgl5 > div > table'

LOGIN_XPATH = '//*[@id="pt1:cb1"]'
SUMMARY_OF_ACCTS_XPATH = '//*[@id="pt1:r5:0:pgl28"]'

SEARCH_ACCOUNT = '//div[contains(@id, "mainAccountNumSearch")]'
SEARCH_METER = '//div[contains(@id, "meterSearch")]'
ACCOUNT_GO_INTERVALS_XPATH = '{}//button[text()="Go"]'.format(SEARCH_ACCOUNT)
ACCOUNT_GO_BILLS_XPATH = '//button[text()="Go"]'

TERMS_SEL = (
        '//div[contains(@id, "content-disclaimer")]' +
        '//span[contains(text(), "Terms and Conditions")]'
)


class UsageViewUnavailableException(Exception):
    pass


class UsageViewBlockedException(Exception):
    pass


class SoCalGasConfiguration(Configuration):
    def __init__(self, account_id, meter_id, scrape_bills=True, scrape_readings=True):
        super().__init__(scrape_bills, scrape_readings)
        self.account_id = account_id
        self.meter_id = meter_id


class SoCalGasScraper(BaseWebScraper):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.browser_name = 'Chrome'
        self.name = 'SoCalGas'
        self.login_url = 'https://business.socalgas.com/portal/'

    @property
    def account_id(self):
        return self._configuration.account_id

    @property
    def meter_id(self):
        return self._configuration.meter_id

    def _execute(self):
        billing_data = None
        interval_data = None

        self._driver.get(self.login_url)
        self._authenticate()

        # Note: While SoCalGas provides GB files (which should include bills), it is not guaranteed
        # that usage/GB will be available for certain meters (particularly those available through Envoy),
        # so we need a reliable way of retrieving bills for all meters - hence scraping the table

        if self.scrape_bills:
            billing_data = self._navigate_and_parse_bill_history()

        if self.scrape_readings:
            self._navigate_to_usage()
            self._accept_terms()
            self._select_account_intervals()

            interval_data = self._download_green_button()

        return Results(bills=billing_data, readings=interval_data)

    def _authenticate(self):
        self.log('AUTHENTICATING')
        self.screenshot('before login')

        self.log('\tWaiting for login page')
        self._driver.wait(10).until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, USERNAME_SEL)
        ))

        # Sometimes form gets redrawn in DOM so references can become stale during filling inputs,
        # so just wait a few seconds and hope it resolves
        self._driver.sleep(2)

        self.log('\tEntering credentials')
        self._driver.fill(USERNAME_SEL, self.username)
        self._driver.fill(PASSWORD_SEL, self.password)

        self.log('\tSubmitting login')
        self._driver.click(LOGIN_XPATH, xpath=True)

        # It's possible that an interstitial page will appear after logging in, prompting the user to
        # enroll in paperless billing. We try to detect that and skip it if it's found
        try:
            skip_button = self._driver.wait().until(EC.visibility_of_element_located((By.PARTIAL_LINK_TEXT, "Skip")))
            self.log('\tHit the "Enroll in Paperless Billing" interstitial, skipping')
            skip_button.click()
        except TimeoutException:
            pass

        self._driver.wait().until(EC.visibility_of_element_located(
            (By.XPATH, SUMMARY_OF_ACCTS_XPATH)
        ))

        self.screenshot('after login')

    def _navigate_to_usage(self):
        self.log('NAVIGATING TO USAGE')
        self.screenshot('before navigating to usage')

        # Can't click on the subnav until hovering on parent nav,
        # so just navigate to that page and go to usage from there.
        # But there are lots of each link, so... fun.
        ways_to_save = (
                '//div[contains(@class, "desktopNav")]' +
                '//a[contains(@href, "faces/waysToSave") and contains(@class, "navParent")]'
        )
        analyze_usage = (
                '//div[contains(@class, "main-content")]' +
                '//a[contains(@href, "waysToSave/analyzeUsage")]'
        )

        self.log('\tClicking "Ways to Save"')
        self._driver.click(ways_to_save, xpath=True)
        self._driver.wait().until(EC.visibility_of_element_located(
            (By.XPATH, analyze_usage)
        ))
        self.screenshot('ways to save')

        self.log('\tClicking "Analyze Usage"')
        self._driver.click(analyze_usage, xpath=True)

        # There are two different things that can happen here - if the account user has
        # not used this page yet, it will ask them to authorize third-party charts..
        # Otherwise, it will load the frame
        find = self._driver.find
        iframe_or_authorize = lambda _: bool(find(IFRAME_SEL) or find(TERMS_SEL, xpath=True))

        self._driver.wait().until(iframe_or_authorize)
        self.screenshot('after navigating to usage')

    def _accept_terms(self):
        self.log('ACCEPTING TERMS AND CONDITIONS')
        if not self._driver.find(TERMS_SEL, xpath=True):
            self.log('\tTerms have been accepted already, skipping')
            return

        accept_sel = (
                '//div[contains(@id, "content-disclaimer")]' +
                '//button[contains(text(), "Accept")]'
        )
        self.log('\tClicking "Accept"')
        self._driver.click(accept_sel, xpath=True)

        # Accepting terms redirects back to "Ways to Save > Usage History",
        # so now we need to navigate BACK to analyze usage
        self._driver.wait().until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, IFRAME_SEL)
        ))
        self.log('\tTerms accepted')
        self.screenshot('after accepting terms')
        self._navigate_to_usage()

    def __select_account(self, go_btn_selector, option_query, shows_right_account):
        self.log('SELECTING ACCOUNT')
        self.screenshot('before selecting account')

        # Might or might not be multiple accounts per a login, so check for the "Go" button
        # specific to account selection and select account if it's present. Avoid the hidden,
        # always-present "meter search" Go button
        #
        # NOTE: Because of race conditions, this go_button *might* be replaced in DOM, so only use
        # as a test for presence and do not interact with it without doing a fresh driver.find
        go_button = self._driver.find(go_btn_selector, xpath=True)

        if go_button:
            self.__pick_account_from_dropdown(
                go_btn_selector,
                option_query,
                shows_right_account)
        else:
            self.log('\tNo account selector present')

    def __pick_account_from_dropdown(self, go_btn_selector, option_query, shows_right_account):
        self.log('\tSelecting account in dropdown')

        # So there is a fascinating race condition here... Some script at some point of loading
        # appears to replace the account selector in the DOM, creating several possible failures:
        #  - If the option is clicked before and "Go" is clicked after re-mount, it reloads default account
        #  - If the option or "Go" btn is selected before and clicked after re-mount, it's a StaleElementError
        #
        # The page is already loaded, so "wait().until()" won't really help,
        # and there do not seem to be any identifying changes in the replaced node,
        # so the best that can really be done is just to wait awhile and hope it's updated
        self._driver.sleep(30)

        option = option_query()

        if not option:
            raise Exception('Can not locate account {} in dropdown'.format(self.account_id))

        option.click()
        self._driver.find(go_btn_selector, xpath=True).click()

        self.log('\tWaiting for account to load')

        try:
            self._driver.wait().until(shows_right_account)
        except Exception as e:
            usage_unavailable = self._driver.find(
                '//*[contains(text(), "Analyze Usage view for your account '
                'is currently unavailable")]',
                xpath=True
            )

            if usage_unavailable:
                self.log('"Analyze Usage" view unavailable for account+meter')
                raise UsageViewUnavailableException()

            usage_blocked_by_basic_questions_form = self._driver.find(
                '//*[contains(text(), "Start saving by answering these basic questions")]',
                xpath=True
            )

            if usage_blocked_by_basic_questions_form:
                self.log('"Analyze Usage" view blocked by "basic questions" form'
                         ' which customer should fill out (rather than us)')
                raise UsageViewBlockedException()

            raise e

        self.log('\tAccount loaded')

    def _select_account_intervals(self):
        def option_query():
            return self._driver.find(
                '{}//option[contains(text(), "{}")]'.format(SEARCH_ACCOUNT, self.account_id),
                xpath=True)

        def shows_right_account(_):
            # Iframe is always present on page, and the only thing that really helps is that the "src" attr
            # has a CustomerId query param that is MOSTLY the account num (it might be a truncated version),
            # so need to match it up by comparing its param against the account num itself
            iframe = self._driver.find(IFRAME_SEL)
            src = iframe.get_attribute('src') if iframe else ''

            # "Stale element" iframe will have <src="javascript:..">
            if not iframe or not src.startswith('http'):
                return False

            customer_id = IFRAME_ACCOUNT_RGX.search(src).group()
            return customer_id in str(self.account_id)

        self.__select_account(
            ACCOUNT_GO_INTERVALS_XPATH,
            option_query,
            shows_right_account)

        # Meter should be selected already - one meter per account?
        # TODO Kevin: Sporadic stale element error on .find
        meter_id = self._driver.find(
            '{}//span[@class="af_selectOneChoice_content"]'.format(SEARCH_METER),
            xpath=True).text

        if meter_id != str(self.meter_id):
            self.log('\tMeter {} is selected instead of {}'.format(meter_id, self.meter_id))
            raise Exception('Meter {} is not selected, can not download data'.format(self.meter_id))

        self.log('\tMeter {} is already selected'.format(meter_id))

    def _select_account_bills(self):
        def option_query():
            return self._driver.find(
                '//option[contains(text(), "{}")]'.format(self.account_id),
                xpath=True)

        def shows_right_account(_):
            # TODO find reliable means of detecting correct DOM state
            self._driver.sleep(20)
            return True

        return self.__select_account(
            ACCOUNT_GO_BILLS_XPATH,
            option_query,
            shows_right_account)

    def _download_green_button(self):
        self.log('DOWNLOADING GREEN BUTTON')
        self._switch_to_iframe()
        self._open_modal()

        start_date, end_date = self._available_history_range()
        zip_files = []

        # Download manager does not allow for longer than 367-day periods,
        # so just download individual yearlong periods
        while start_date < end_date:
            current_end = start_date + timedelta(days=364)

            if end_date < current_end:
                current_end = end_date

            self._enter_dates(start_date, current_end)
            self._prepare_export(start_date)
            zip_files.append(self._download_zip_file())

            start_date = current_end + timedelta(days=1)

        self.log('\tDownloaded {} zip files'.format(len(zip_files)))
        return self._process_zip_files(zip_files)

    def _switch_to_iframe(self):
        # There are sporadic issues switching to the frame, but a simple sleep seems to help
        self.log('\tWaiting 90 seconds for usage UI to load')
        self._driver.sleep(90)
        self.log('\tSwitching to iframe')
        self._driver.switch_to.frame(self._driver.find(IFRAME_SEL))

        # Wait for iframe contents to load
        self._driver.wait().until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, GREEN_BUTTON_SEL)
        ))

        # Wait for occasional overlay to disappear
        self._driver.wait().until(EC.invisibility_of_element_located(
            (By.CSS_SELECTOR, 'div.acl-hi-modal-overlay')
        ))
        self.screenshot('after switching to frame')

    def _open_modal(self):
        self.log('\tOpening GreenButton modal')
        self._driver.click(GREEN_BUTTON_SEL)
        self._driver.wait().until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, START_INPUT_SEL)
        ))
        self.screenshot('after opening modal')

    def _available_history_range(self):
        # The GreenButton modal helpfully tells when the earliest and latest dates with
        # readings are, so get those to compare against scraper start/end dates because
        # scraper will fail if trying to use dates outside of that range.
        # Dates will be at end of sentence, like "Meter history is available starting: 01/12/2016"
        def get_label_date(label_sel):
            return date_parser.parse(
                self._driver.find(label_sel).text.split(' ')[-1]
            ).date()

        self.log('\tEntering dates')

        history_start = get_label_date('label[for="lblHistory"]')
        history_end = get_label_date('label[for="lblRecentAvaliableDataDate"]')

        if history_start > self.start_date:
            self.log('\tHistory begins after start date, scraping from date {}'.format(
                self._date_str(history_start)
            ))
            start_date = history_start
        else:
            self.log('\tHistory includes start date')
            start_date = self.start_date

        if history_end < self.end_date:
            self.log('\tHistory ends before end date, scraping to date {}'.format(
                self._date_str(history_end)
            ))
            end_date = history_end
        else:
            self.log('\tHistory includes end date')
            end_date = self.end_date

        return (start_date, end_date)

    def _enter_dates(self, start_date, end_date):
        # Cannot type dates into these input fields, so just use script to set value
        # rather than opening a calendar and cycling through buttons and pages
        def set_value(_id, dt):
            self._driver.execute_script(
                'document.getElementById("{}").value = "{}"'.format(_id, self._date_str(dt))
            )

        set_value('txtStartDate', start_date)
        set_value('txtEndDate', end_date)

    def _prepare_export(self, start_date):
        self.log('\tPreparing export starting {}'.format(self._date_str(start_date)))
        self._driver.click('a#la-gb-export')
        self._driver.wait(300).until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, 'a#lnkDownload')
        ))

    def _download_zip_file(self):
        # While downloading, grab the filename from the "download instructions" section
        instructions = self._driver.find('p#downloadInstructions').text
        download_name = DOWNLOAD_FILENAME_RGX.search(instructions).group()
        path = self._zip_path(download_name)

        self.log('\t\tDownloading file {}'.format(download_name))
        self._driver.click('a#lnkDownload')
        self._driver.wait().until(lambda _: os.path.exists(path))
        self.log('\t\tFile downloaded')

        return download_name

    def _process_zip_files(self, filenames):
        self.log('EXTRACTING READINGS FROM DOWNLOADS')
        interval_data = {}

        # Extracted XML filenames might not match folder name, so just extract all
        # and then process all XML files found
        for filename in filenames:
            self._unzip_archive(filename)

        for filename in os.listdir(self._driver.download_dir):
            if not filename.endswith('.xml'):
                self.log('\tSkipping parse of {}'.format(filename))
                continue

            self.log('\tParsing file "{}"'.format(filename))
            parsed = gbparser.parse('{}/{}'.format(self._driver.download_dir, filename))

            if 'readings' in parsed:
                self.log('\tReturned data {}'.format(parsed))
                interval_data.update(parsed['readings'])
            else:
                self.log('\tNo data returned', level='warning')

        return interval_data

    def _unzip_archive(self, filename):
        self.log('\tGetting readings from {}'.format(filename))
        path = self._zip_path(filename)

        with ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(self._driver.download_dir)

        os.remove(path)

    def _zip_path(self, filename):
        return '{}/{}.zip'.format(self._driver.download_dir, filename)

    def _parsed_bill_history(self, rows) -> BillingData:

        def billing_period(range_txt: str, billed_days_txt: str) -> Tuple[date, date]:
            # Example `range_txt`: "03/20/2018 - 04/18/2018 Corrected"
            # Note: Switching to using date_parser here instead of more manual
            # parsing, which was breaking due to a change in date format (2-digit
            # years instead of 4).
            parts = range_txt.split()
            end_txt = parts[2]
            end = date_parser.parse(end_txt).date()
            start = end - timedelta(days=(int(billed_days_txt) - 1))
            return (start, end)

        def cost(td: str) -> float:
            stripped = (td
                        .replace('$', '')
                        .replace(' ', '')
                        .replace(',', ''))

            # Invert credits and drop label, e.g: "21.36CR" -> "-21.36"
            return float(stripped
                         if stripped[-2:] != 'CR'
                         else '-' + stripped[:-2])

        def used(td: str) -> float:
            return float(td.replace(',', ''))

        def with_amendments(all_rows) -> List[List[str]]:
            '''Filter out periods that've been adjusted more recently'''

            def to_cells(r) -> List[str]:
                return [td.text for td in r.find_elements_by_tag_name('td')
                        if td.is_displayed()]

            def corrected_or_first(rows: List[List[str]]) -> List[str]:
                return next((r for r in rows if 'Corrected' in r[1]), rows[0])

            # Extract the date range from a table row
            def by_range(row):
                # The raw range may have some trailing text, e.g. 'Corrected'
                # We split on whitespace and pull out just the date parts
                date_range_raw = row[1]
                parts = date_range_raw.split()
                start = parts[0]
                end = parts[2]
                return (start, end)

            rows_as_cells = sorted((to_cells(r) for r in all_rows), key=by_range)

            by_period = {k: list(v) for k, v in groupby(rows_as_cells, by_range)}

            return [corrected_or_first(grp) for grp in by_period.values()]

        def to_billing_datum(cells: List[str]) -> BillingDatum:
            start, end = billing_period(cells[1], cells[2])

            # Unused fields:
            # --------------
            # date_mailed = cells[0]
            # total_amount_due = cells[5]

            return BillingDatum(
                start=start,
                end=end,
                cost=cost(cells[4]),
                used=used(cells[3]),
                peak=None,
                items=None,
                attachments=None)

        return [to_billing_datum(r) for r in with_amendments(rows)]

    def _navigate_and_parse_bill_history(self) -> BillingData:
        self.log('OPENING "My Bill" POP-OVER MENU')

        self._driver.execute_script(
            '$("%s").addClass("highlight-parent")' % MY_BILL_MENU_HEADER_SEL)

        self.screenshot('after hovering "My Bill" pop-over menu')

        self.log('NAVIGATING TO BILL HISTORY')
        self._driver.click(BILL_HISTORY_LINK_SEL)

        try:
            (self._driver
                .wait(20)
                .until(EC.visibility_of_element_located(
                (By.CSS_SELECTOR, BILL_HISTORY_TABLE_SEL))))
        except TimeoutException:
            # There's a chance that this account simply doesn't have billing history,
            # so return empty array if it doesn't...
            no_activity_sel = ('//div[contains(text(), '
                               '"There has been no billing activity on this account."'
                               ')]')
            if self._driver.find(no_activity_sel, xpath=True):
                self.log('No billing activity for account, exiting')
                return []

            # ... but any other time out should still error out
            raise

        self.log('\tArrived at Bill History page')
        self.screenshot('after navigating to Bill History page')

        self._select_account_bills()

        self.log('\tScraping bill data from table')
        rows = self._driver.find_elements_by_css_selector(
            '{} > tbody > tr'.format(BILL_HISTORY_TABLE_SEL))

        self.log('\tParsing %s rows of bill history data' % len(rows))
        bill_history = self._parsed_bill_history(rows)
        self.log('\t%s rows remain after SoCal\'s billing corrections' % len(bill_history))

        self.log('\tAsserting bill history periods are without overlaps')
        assert_is_without_overlaps(bill_history, log=self.log)

        return bill_history


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = SocalGasConfiguration(meter_id=meter.service_id)

    return run_datafeed(
        SocalGasScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
