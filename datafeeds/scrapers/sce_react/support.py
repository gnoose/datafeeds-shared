import logging
from typing import Tuple

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException

from datafeeds.common.base import BaseWebScraper

log = logging.getLogger(__name__)


def detect_and_send_escape_to_close_survey(driver, timeout=5):
    try:
        locator = (By.CLASS_NAME, "fsrAbandonButton")
        elem = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(locator)
        )
        actions = ActionChains(driver)
        actions.send_keys_to_element(elem, Keys.ESCAPE)
        log.info("popup closed")
        driver.sleep(1)
        return True
    except Exception:
        pass


def detect_and_close_survey(driver, timeout=5):
    try:
        locator = (By.CLASS_NAME, "fsrDeclineButton")
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(locator)
        ).click()
        log.info("click decline button")
        driver.sleep(2)
        return True
    except Exception as exc:
        log.info("exception closing survey: %s", exc)


def detect_and_close_modal(driver, timeout=5):
    try:
        locator = (By.CSS_SELECTOR, '#graphHeader button[aria-label="close dialog"]')
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(locator)
        ).click()
        log.info("modal closed")
        driver.sleep(1)
        return True
    except Exception:
        pass


def dismiss_overlay_click(
    driver, locator: Tuple[By, str] = None, elem: WebElement = None, retries: int = 5
):
    """If element is blocked by a modal, attempt to close it."""
    if elem:
        pass
    elif locator:
        elem = WebDriverWait(driver, 10).until(EC.presence_of_element_located(locator))
    else:
        raise ValueError("either element, or locator must be provided")

    for i in range(retries):
        try:
            elem.click()
            break
        except ElementClickInterceptedException:
            log.info(
                "blocked by overlay, attempting to close before clicking again (%s/%s)",
                i,
                retries,
            )
            if detect_and_send_escape_to_close_survey(driver):
                pass
            elif detect_and_close_survey(driver):
                pass
            elif detect_and_close_modal(driver):
                pass
            else:
                log.info("unable to close overlay, raising")
                raise
    driver.screenshot(BaseWebScraper.screenshot_path("close overlay"))
