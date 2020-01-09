"""Some Selenium utility functions/classes

This module provides some simple Selenium utility functions
and classes, e.g. custom wait predicates, content managers
for iframes/windows, etc.
"""
import os
import re

from io import BytesIO
from PIL import Image
from selenium.common.exceptions import InvalidElementStateException


# Code to stitch full page screenshots together, credit to:
# https://gist.github.com/fabtho/13e4a2e7cfbfde671b8fa81bbe9359fb


def whole_page_screenshot(driver, filename):
    # from here http://stackoverflow.com/questions/1145850/how-to-get-height-of-entire-document-with-javascript
    js = (
        "return Math.max( document.body.scrollHeight, document.body.offsetHeight, "
        + "document.documentElement.clientHeight,  document.documentElement.scrollHeight, "
        + "document.documentElement.offsetHeight);"
    )
    scrollheight = driver.execute_script(js)

    slices = []
    offset = 0
    try:
        while offset < scrollheight:
            driver.execute_script("window.scrollTo(0, %s);" % offset)
            img = Image.open(BytesIO(driver.get_screenshot_as_png()))
            offset += img.size[1]
            slices.append(img)

        with Image.new("RGB", (slices[0].size[0], offset)) as screenshot:
            offset = 0
            for img in slices:
                screenshot.paste(img, (0, offset))
                offset += img.size[1]
            screenshot.save(filename)
    finally:
        for img in slices:
            img.close()


class ec_in_frame:
    """Evaluate an expected condition within the context of a given iframe

    Args:
        iframe: An iframe locator (anything that can be passed to
            "driver.switch_to.frame" can also be used here, e.g.
            an iframe string id, a Selenium locator, etc).
        condition: A selenium ExpectedCondition
    """

    def __init__(self, iframe, condition):
        self.iframe = iframe
        self.condition = condition

    def __call__(self, driver):
        with IFrameSwitch(driver, self.iframe):
            return self.condition


class ec_or:
    """Construct an expected condition which is the logical OR of multiple other conditions

    The provided conditions are evaluated in the order provided. The first condition to evaluate
    to "True" is returned. Otherwise, None is returned.
    """

    def __init__(self, *conditions):
        self.conditions = conditions

    def __call__(self, driver):
        for condition in self.conditions:
            try:
                result = condition(driver)
                if result:
                    return result
            except:  # noqa=E722
                pass
        return None


class ec_and:
    """Construct an expected condition which is the logical AND of multiple other conditions

    The provided conditions are evaluated in the order provided. If all conditions evaluate to True,
    then a list of condition return values is returned by this function (one for each condition
    evaluate). Else, None is returned.
    """

    def __init__(self, *conditions):
        self.conditions = conditions

    def __call__(self, driver):
        results = []
        for condition in self.conditions:
            try:
                result = condition(driver)
                if not result:
                    return None
                results.append(result)
            except:  # noqa=E722
                return None
        return results


def scroll_to(driver, elem):
    driver.execute_script("arguments[0].scrollIntoView();", elem)


class element_text_doesnt_contain:
    def __init__(self, locator, value):
        self.locator = locator
        self.value = value

    def __call__(self, driver):
        element = driver.find_element(*self.locator)
        return self.value not in element.text


class element_cleared:
    def __init__(self, locator):
        self.locator = locator

    def __call__(self, driver):
        element = driver.find_element(*self.locator)
        try:
            element.clear()
            return True
        except InvalidElementStateException:
            return False


class window_count_equals:
    def __init__(self, count):
        self.count = count

    def __call__(self, driver):
        return len(driver.window_handles) == self.count


class file_exists_in_dir:
    """Wait until a file matching a regex appears in a directory.

    This is passed to a Selenium wait().until construct. This takes a
    directory name and a regular expression.

    Args:
        directory (str): The path of a directory to monitor
        pattern (str): A regular expression
    """

    def __init__(self, directory, pattern):
        self.directory = directory
        self.pattern = re.compile(pattern)

    def __call__(self, driver):
        for path in os.listdir(self.directory):
            if self.pattern.match(path):
                return path
        return False


class WindowSwitch:
    """Simple context manager for Selenium windows

    You can use this to temporarily switch to an iframe, then go back
    to the default content, using a "with" statement.

    E.g.:
        with WindowSwitch(driver, window_handle):
            <do stuff in the new window>

    Args:
        driver: A Selenium WebDriver
        target_handle: The handle referring to the window to switch to
    """

    def __init__(self, driver, target_handle, close=False):
        self.driver = driver
        self.target_handle = target_handle
        self.previous_window_handle = None
        self.close = close

    def __enter__(self):
        self.previous_window_handle = self.driver.current_window_handle
        self.driver.switch_to.window(self.target_handle)

    def __exit__(self, *args):
        if self.close:
            self.driver.close()
        self.driver.switch_to.window(self.previous_window_handle)


class IFrameSwitch:
    """Simple context manager for Selenium iframes.

    You can use this to temporarily switch to an iframe, then go back
    to the default content, using a "with" statement.

    E.g.:
        with IFrameSwitch(driver, "iframe id"):
            <do stuff in the new iframe>

    Args:
        driver: A Selenium WebDriver
        iframe: An iframe locator (anything that can be passed to
            "driver.switch_to.frame" can also be used here, e.g.
            an iframe string id, a Selenium locator, etc).
    """

    def __init__(self, driver, iframe):
        self.driver = driver
        self.iframe = iframe

    def __enter__(self):
        self.driver.switch_to.frame(self.iframe)

    def __exit__(self, *args):
        self.driver.switch_to.default_content()


def clear_downloads(download_dir):
    """Clean files from the download directory."""
    to_remove = []
    for filename in os.listdir(download_dir):
        if (
            filename.endswith(".zip")
            or filename.endswith(".csv")
            or filename.endswith(".xlsx")
        ):
            to_remove.append(os.path.join(download_dir, filename))

    for path in to_remove:
        os.remove(path)
