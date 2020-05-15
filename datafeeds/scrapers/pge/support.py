import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException
from datafeeds.common.util.selenium import scroll_to

log = logging.getLogger(__name__)


def wait_for_block_overlay(driver, seconds=30):
    condition = EC.invisibility_of_element_located(
        (By.CSS_SELECTOR, ".blockUI.blockOverlay")
    )
    driver.wait(seconds).until(condition)


def wait_for_account(driver):
    # Main account homepage after login
    driver.wait().until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#accountUserName"))
    )
    # The overlay will flicker several times before the page is fully loaded,
    # so try to sleep through the first few series of flickers
    driver.sleep(5)
    wait_for_block_overlay(driver, 90)


def wait_for_accounts_list(driver):
    # Main account homepage after login
    driver.wait().until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#accountListItems"))
    )
    # The overlay will flicker several times before the page is fully loaded,
    # so try to sleep through the first few series of flickers
    driver.sleep(5)
    wait_for_block_overlay(driver, 90)


def click(
    driver,
    css_selector: str = None,
    xpath: str = None,
    elem: WebElement = None,
    scroll: bool = True,
):
    """helper method to click an element, if it is blocked by blockOverlay, waits for the overlay to disappear"""
    if elem:
        pass
    elif css_selector:
        elem = driver.find_element_by_css_selector(css_selector)
    elif xpath:
        elem = driver.find_element_by_xpath(xpath)
    else:
        raise ValueError("one of css_selector, xpath or elem must be provided")

    retries_left = 5
    while retries_left > 0:
        try:
            scroll_to(driver, elem) if scroll is True else None
            elem.click()
            break
        except ElementClickInterceptedException as e:
            if "blockUI blockOverlay" in e.msg:
                log.info(
                    "blocked by overlay, waiting for it to go before clicking again"
                )
                wait_for_block_overlay(driver)
                continue
            else:
                raise

        finally:
            retries_left -= 1
