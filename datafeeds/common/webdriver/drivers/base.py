"""
Base class for all browser-specific drivers (ChromeDriver, etc),
providing a unified, more convenient API for numerous packages and tasks,
proxying any other attr/method calls directly to the driver.


Normally it would make sense to just `return self` after the methods
so that they can be chained, but to cut down on the level of abstraction
some methods return Selenium objects like Select to be used directly,
so all methods should just return the results of Selenium calls.


Web driver should be used as a context manager, so that the various
cleanup items can be hidden and run automatically, eg:

    with ChromeDriver() as driver:
        elem = driver.find(some_selector)

This will handle:
    - Initializing and quitting the web driver instance

But the driver can also be used manually:

    driver = ChromeDriver()
    driver.start()
    elem = driver.find(some_selector)
    driver.stop()
"""

from abc import ABC as Abstract, abstractmethod
import os
import shutil
import time
import logging

from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import ui
from selenium.webdriver.support.select import Select

from datafeeds.common.util import selenium as utils


log = logging.getLogger(__name__)


class BaseDriver(Abstract):
    @abstractmethod
    def _options(self):
        pass

    def __init__(self, outputpath):
        # Drivers need to have a few directories set up - one for browser downloads
        # and another for data
        # TODO Kevin: where is the "<log>/data" directory used?
        self.download_dir = "{}/current".format(outputpath)
        self.data_dir = "{}/data".format(outputpath)
        self._outputpath = outputpath

        for path in [self.download_dir, self.data_dir]:
            if os.path.exists(path):
                shutil.rmtree(path)

            os.makedirs(path)

    def __getattr__(self, name):
        """
        Proxy missing methods/attributes to driver, so that the Selenium API
        can be used as-is without requiring wrappers
        """
        return getattr(self._driver, name)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    def start(self):
        log.info("Starting webdriver")

    def stop(self):
        log.info("Stopping webdriver")
        self._driver.quit()

    def get(self, url):
        """
        Connect to a given URL
        """
        return self._driver.get(url)

    def screenshot(self, outputpath, whole=False):
        """
        Take a screenshot of the current page.
        If whole is indicated, driver will scroll through entire page.
        Otherwise, only the portion visible in display will be captured.
        """
        log.info("creating screenshot %s (whole=%s)", outputpath, whole)
        if whole:
            return utils.whole_page_screenshot(self._driver, outputpath)

        return self._driver.save_screenshot(outputpath)

    def find(self, selector, xpath=False):
        """
        Find element matching a given selector or None.
        Useful for existence checks that do not need reference to element, eg:

          if driver.find(selector):
            # do other stuff
        """
        try:
            return self.find_or_raise(selector, xpath=xpath)

        except NoSuchElementException:
            return None

    def find_or_raise(self, selector, xpath=False):
        """
        Do not catch exception, unlike self.find
        """
        if xpath:
            return self._driver.find_element_by_xpath(selector)

        return self._driver.find_element_by_css_selector(selector)

    def find_all(self, selector, xpath=False):
        """
        Find all elements matching a given selector or [].
        """
        if xpath:
            return self._driver.find_elements_by_xpath(selector)

        return self._driver.find_elements_by_css_selector(selector)

    def get_select(self, selector, xpath=False):
        """
        Find and and return a Select element
        """
        elem = self.find(selector, xpath=xpath)
        return Select(elem) if elem else None

    def fill(self, selector, text, xpath=False):
        """
        Find and populate an input element or raise error if unable to locate.
        Returns result of Selenium WebDriver .send_keys()
        """
        return self.find_or_raise(selector, xpath=xpath).send_keys(text)

    def clear(self, selector, xpath=False):
        """
        Find and clear an input element or raise error if unable to locate.
        Returns result of Selenium WebDriver .clear()
        """
        return self.find_or_raise(selector, xpath=xpath).clear()

    def click(self, selector, xpath=False):
        """
        Find and click an element or raise error if unable to locate.
        Returns result of Selenium WebDriver .click()
        """
        return self.find_or_raise(selector, xpath=xpath).click()

    def wait(self, seconds=60):
        """
        Returns instance of selenium.webdriver.support.ui.WebDriverWait
        """
        return ui.WebDriverWait(self._driver, seconds)

    def sleep(self, seconds):
        """
        Alias for time.sleep
        """
        return time.sleep(seconds)
