import logging
from typing import List, Optional

from datafeeds.common import Timeline
from datafeeds.common.base import BaseWebScraper
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Results
from datafeeds.common.support import Configuration
from datafeeds.common.typing import Status

from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)


class SCLMeterWatchConfiguration(Configuration):
    def __init__(self, meter_numbers: List[str]):
        super().__init__(scrape_readings=True)
        self.meter_numbers = meter_numbers


class SCLMeterWatchScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "SCL MeterWatch"
        self.url = "http://smw.seattle.gov"

    def _execute(self):
        self._driver.get(self.url)
        timeline = Timeline(self.start_date, self.end_date)
        """
            - go to http://smw.seattle.gov
            - login
            - get popup window
        """
        for meter_number in self._configuration.meter_numbers:
            """
            - choose account from Meter OR Meter Group dropdown (option value == meter_number)
            - set dates in From Date, To Date (04/01/2020 format) (self.start_date, self.end_date)
              - check available dates (Starts on 04/30/2018, Ends of 03/18/2020)
              - continue if requested range not available
            - click Download Data button
              - saves to config.WORKING_DIRECTORY/15_minute_download.csv
            - open file, read kWh Usage with csv into timeline (see energymanager_interval.py#221)
              - get existing at datetime (timeline.lookup)
              - timeline.insert current value + existing
              - sample data:
"Starting Date","Ending Date","Starting Time","Ending Time","kWh Cost","kWh Usage","kvarh"
"03/01/2020","03/01/2020","00:00:01","00:15:00","$4.80","48.600","12.060"
"03/01/2020","03/01/2020","00:15:01","00:30:00","$4.85","49.140","12.060"              
            """
            pass
        return Results(readings=timeline.serialize())


"""
BEGIN: old Firefox scraper for reference; remove when no longer needed

convert etl_logging.get().debug(...) to log.debug(...)
"""
"""
def _connect_and_download_data(username, password, service_id, start_date, end_date):
    firefox_profile = _build_profile(service_id)
    driver = _connect_to_firefox(firefox_profile)
    main_window = None
    login_window = None

    try:
        etl_logging.get().debug('Navigating to meter watch...')
        driver.get("http://smw.seattle.gov")

        #Meterwatch immediatly spawns a popup when loaded which is the actual
        #window we want. So we have to go and grab the main window handle and
        #THEN go looking for the popup window and switch to it. We need
        #to hold on to that main window handle to properly destroy everything
        #later.
        etl_logging.get().debug('Grabbing main window...')
        main_window = _get_main_window(driver)

        # Click the "Go" button to launch the login window.
        driver.find_element_by_id('divLoginButton').click()

        login_window = None
        while not login_window:
            for handle in driver.window_handles:
                if handle != main_window:
                    login_window = handle
                    break

        #We have our popup, so lets do stuff with it.
        driver.switch_to.window(login_window)
        etl_logging.get().debug("Driver title: " + driver.title)
        assert 'Seattle MeterWatch' in driver.title

        _login_to_meterwatch(driver, username, password)
        _choose_account(driver, service_id)
        _choose_dates(driver, start_date, end_date)
        _download_data(driver, service_id)
    finally:
        driver.quit()

def _build_profile(service_id):
    #In order to actually download files we have to use a bit of a hack.
    #Firefox by default puts up a dialog asking for a location
    #to save to, which is an issue as selenium can't respond to a native
    #dialog. Instead we setup a profile that allows us to bypass that
    #dialog and download straight away.
    etl_logging.get().debug('Creating firefox profile...')
    rval = webdriver.FirefoxProfile()

    rval.set_preference("browser.download.folderList", 2)
    rval.set_preference("browser.download.manager.showWhenStarting", False)

    # Allow popups
    rval.set_preference("dom.disable_open_during_load", False)
    rval.set_preference("dom.disable_beforeunload", False)

    outputpath = _outputpath(service_id)
    os.makedirs(outputpath)
    etl_logging.get().debug('Setting download dir to {}'.format(outputpath))
    rval.set_preference("browser.download.dir", outputpath)

    rval.set_preference(
        'browser.helperApps.neverAsk.saveToDisk',
        'application/excel'
    )

    return rval


def _get_main_window(driver):
    #The window handle is often not immediatly set, this goes
    #into a loop until the handle is set. We need it to
    #differeniate from the popup later.
    rval = None
    elapsed = 0
    start_time = datetime.utcnow()

    while not rval and elapsed < 60:
        rval = driver.current_window_handle
        elapsed = (datetime.utcnow() - start_time).seconds

    if not rval:
        raise Exception('Unable to get main window, dying...')

    return rval

def _login_to_meterwatch(driver, username, password):
    etl_logging.get().debug('Logging in to meterwatch...')

    try:
        driver.find_element_by_xpath(u'//input[@id="P101_USERNAME"]').send_keys(username)
        driver.find_element_by_xpath(u'//input[@id="P101_PASSWORD"]').send_keys(password)
        driver.find_element_by_xpath(u'//img[@alt="Login to Seattle MeterWatch"]').click()

        ui.WebDriverWait(driver, 10).until(
            EC.title_contains('Seattle MeterWatch : Display Meter Data')
        )
    except Exception:
        raise Exception('Unable to login, invalid credentials?')

def _choose_account(driver, service_id):
    etl_logging.get().debug('Selecting correct account...')
    select = ui.Select(driver.find_element_by_name('p_t04'))
    meter_id = None
    for option in select.options:
        if option.text.startswith(service_id):#'669086'):
            etl_logging.get().debug('Found account option: {}'.format(option.text))
            meter_id = option.get_attribute('value')
            select.select_by_value(meter_id)
            break


    ui.WebDriverWait(driver, 10).until(
        EC.element_to_be_selected(
            driver.find_element_by_xpath(u'//option[@value="{}"]'.format(meter_id)),
        )
    )

def _choose_dates(driver, start_date, end_date):
    available_from_elem = driver.find_element_by_xpath(u'//label[@for="P10_FROM_DATE"]//span')
    available_from_text = available_from_elem.text.split(' ')[-1]
    available_from = datetime.strptime(available_from_text, '%m/%d/%Y')

    available_to_elem = driver.find_element_by_xpath(u'//label[@for="P10_THROUGH_DATE"]//span')
    available_to_text = available_to_elem.text.split(' ')[-1]
    available_to = datetime.strptime(available_to_text, '%m/%d/%Y')

    if start_date.date() < available_from.date():
        start_date = available_from

    if end_date.date() > available_to.date():
        end_date = available_to

    etl_logging.get().debug('Setting start date to: {}'.format(start_date.strftime('%m/%d/%Y')))
    etl_logging.get().debug('Setting end date to: {}'.format(end_date.strftime('%m/%d/%Y')))

    driver.find_element_by_xpath(u'//input[@id="P10_FROM_DATE"]').send_keys(start_date.strftime('%m/%d/%Y'))
    driver.find_element_by_xpath(u'//input[@id="P10_THROUGH_DATE"]').send_keys(end_date.strftime('%m/%d/%Y'))

def _download_data(driver, service_id):
    etl_logging.get().debug('Beginning download...')
    driver.find_element_by_xpath(u'//a[@href="javascript:apex.submit(\'DOWNLOAD\');"]').click()

    started_at = datetime.utcnow()
    duration = 0
    target_items = []

    while len(target_items) != 1 and duration < 180:
        target_items = glob.glob('{}/*.csv*'.format(_outputpath(service_id)))
        duration = (datetime.utcnow() - started_at).seconds

        etl_logging.get().debug('Downloading progress: {}'.format(os.listdir(_outputpath(service_id))))
        time.sleep(1)

    if len(target_items) != 1:
        raise Exception('Unable to download file...')

def _outputpath(service_id):
    return "{0}/{1}".format(etl_logging.get().outputpath, service_id)
"""
"""
END: old Firefox scraper for reference; remove when no longer needed
"""


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:

    meter_numbers = [meter.service_id]
    # if totalized is set in meta, get list of meter numbers
    totalized = (datasource.meta or {}).get("totalized")
    if totalized:
        meter_numbers = totalized.split(",")
    configuration = SCLMeterWatchConfiguration(meter_numbers=meter_numbers)

    return run_datafeed(
        SCLMeterWatchScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
