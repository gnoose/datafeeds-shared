import json
import logging
import re
import time

import requests
from selenium.webdriver.remote.webelement import WebElement

from datafeeds import config
from datafeeds.common.exceptions import ScraperPreconditionError
from datafeeds.common.webdriver.drivers.base import BaseDriver

log = logging.getLogger(__name__)


class CaptchaFailed(Exception):
    pass


def recaptcha_v2(driver: BaseDriver, iframe_parent: WebElement, page_url: str):
    """Use 2captcha.com (https://2captcha.com/2captcha-api) to solve captchas.

    Send request to 2captcha.com with cookies and request; wait up to a minute for a response.
    """
    # try to get key from iframe src attribute
    iframe = driver.find_element_by_tag_name("iframe")
    iframe_url = iframe.get_attribute("src")
    log.info("starting recaptcha_v2: iframe_url=%s", iframe_url)
    captcha_key = None
    if "?" in iframe_url:
        # should be https://www.google.com/recaptcha/api2/anchor?ar=1&..., but might be javascript:false
        (url, params) = iframe_url.get_attribute("src").split("?")
        for param in params.split("&"):
            (key, value) = param.split("=")
            if key == "k":
                captcha_key = value
    else:
        html = iframe_parent.get_attribute("innerHTML")
        match = re.search(r'.*?iframe src=".*?k=(.*?)\&.*?"', html)
        if match:
            captcha_key = match.group(1)
        log.info("captcha key: trying innerHTML: %s", html)

    if not captcha_key:
        raise ScraperPreconditionError("unable to find captcha key")

    log.info("found captcha key %s", captcha_key)
    # get cookies
    cookies = {}
    for cookie in driver.get_cookies():
        cookies[cookie["name"]] = cookie["value"]
    # join with ; (ie name1=value1; name2=value2)
    cookie_str = "; ".join([k + "=" + v for k, v in cookies.items()])
    params = {
        "key": config.CAPTCHA_API_KEY,
        "method": "userrecaptcha",
        "googlekey": captcha_key,
        "pageurl": page_url,
        "cookies": cookie_str,
        "json": 1,
    }
    resp = requests.post("https://2captcha.com/in.php", data=params)
    log.debug("captcha response=%s", resp.text)
    req_id = json.loads(resp.text).get("request")
    log.info("waiting 30s for a human to solve the captcha")
    time.sleep(30)
    params = {
        "key": config.CAPTCHA_API_KEY,
        "action": "get",
        "id": req_id,
        "json": 1,
    }
    answer = None
    for idx in range(5):
        log.info("get captcha answer %s", idx + 1)
        text = requests.get("https://2captcha.com/res.php", params=params).text
        log.info("captcha response = %s", text)
        response = json.loads(text)
        answer = response.get("request")
        # error messages look like CAPCHA_NOT_READY
        if not re.match(r"^[A-Z_]+$", answer):
            break
        log.info("trying again in 12s")
        time.sleep(12)
    if answer == "CAPCHA_NOT_READY" or "error_text" in response:
        error = "%s: %s" % (answer, response.get("error_text", ""))
        log.warning("error solving captcha: %s", error)
        raise CaptchaFailed(error)
    log.info("setting captcha answer=%s", answer)
    driver.execute_script(
        'document.getElementById("g-recaptcha-response").innerHTML="%s";' % answer
    )
    time.sleep(5)
    return True
