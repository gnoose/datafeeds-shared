import argparse
import logging
import re
import sys
import time
import pprint

from selenium import webdriver
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait

from datafeeds.common.webdriver.virtualdisplay import VirtualDisplay

from datafeeds import config
from datafeeds.smd.models import MeterSummary, AccountSummary, AuthorizationSummary


log = logging.getLogger(__name__)


class LoginFailure(Exception):
    """This exception is raised when the supplied credentials were invalid."""

    pass


class SiteError(Exception):
    """This exception is raised when the SMD site loads with an error message."""

    pass


class AccountParseFailure(Exception):
    """This exception is raised when we couldn't discern account identifiers."""

    pass


class NoAccountsFailure(Exception):
    """This exception is raised when PG&E's site didn't show any accounts/meters to authorize."""


class LoginPage:
    def __init__(self, driver):
        self.driver = driver

    def login(self, username, password):
        self.driver.get("https://sharemydata.pge.com/myAuthorization/login")

        username_textbox = self.driver.find_element_by_id("username")
        password_textbox = self.driver.find_element_by_id("password")
        sign_in_button = self.driver.find_element_by_id("smd-login-main")

        username_textbox.send_keys(username)
        password_textbox.send_keys(password)

        sign_in_button.click()

    def login_successful(self):
        # Sometimes PG&E wants users to rotate their credentials. We need to detect this as well as bad credentials.
        failure_texts = [
            "//div[contains(text(), 'Account disabled')]",
            "//div[contains(text(), 'Your last sign in')]",
        ]

        for pattern in failure_texts:
            try:
                WebDriverWait(self.driver, 5).until(
                    ec.visibility_of_element_located((By.XPATH, pattern))
                )
                return False
            except:  # noqa E722
                pass

        try:
            WebDriverWait(self.driver, 5).until(
                ec.visibility_of_element_located((By.ID, "smd-error-login"))
            )
            return False
        except TimeoutException:
            return True


class HomeScreen:
    def __init__(self, driver):
        self.driver = driver

    def wait_until_ready(self):
        # Unfortunately, there appears to be a JS race condition here. Have to wait for angular JS to run.
        time.sleep(3)

        # This next wait may not be necessary. It was added as an extra check that the page was fully loaded.

        # Authorization boxes are the last to load on the page.
        # Wait to see if *any* "edit pencil" logos appear before deciding
        # whether the CE authorization is present.
        try:
            WebDriverWait(self.driver, 10).until(
                ec.element_to_be_clickable((By.ID, "smd-edit"))
            )
        except TimeoutException:
            log.info(
                "Timed out waiting for SMD edit button. There are likely no authorizations for this login."
            )

        # Make sure the spinner is gone.
        WebDriverWait(self.driver, 120).until(
            ec.invisibility_of_element_located((By.CLASS_NAME, "overlay"))
        )

    def is_authorized(self, provider):
        if provider == "gridium":
            provider_display = "Gridium"
        else:
            provider_display = "Commercial Energy of Montana"

        return provider_display in self.driver.page_source

    def browse_to_authorizations(self, provider):
        if provider == "gridium":
            provider_display = "Gridium "
        else:
            provider_display = "Commercial Energy of Montana, Inc. "

        xpath = '//thead//span[text()="%s"]/../..//*[@id="smd-edit"]' % provider_display
        edit_button = self.driver.find_element_by_xpath(xpath)
        edit_button.click()

        return AuthorizationPage(self.driver)

    def select_provider(self, provider):
        WebDriverWait(self.driver, 60).until(
            ec.presence_of_element_located((By.CLASS_NAME, "ui-select-toggle"))
        ).click()

        if provider == "gridium":
            registration_text = "Gridium"
        else:
            registration_text = "Commercial Energy of Montana, Inc."
        xpath_selector = (
            By.XPATH,
            "//div[contains(@class, 'ui-select-choices-row')]"
            + "//div[contains(text(), '"
            + registration_text
            + "')]",
        )

        WebDriverWait(self.driver, 5).until(
            ec.presence_of_element_located(xpath_selector)
        ).click()

    def browse_to_auth_config_page(self):
        next_button = self.driver.find_element_by_id("smd-next")
        next_button.click()

        return AuthorizationPage(self.driver)


class AuthorizationPage:
    def __init__(self, driver):
        self.driver = driver

    def wait_until_ready(self):
        WebDriverWait(self.driver, 120).until(
            ec.invisibility_of_element_located((By.CLASS_NAME, "overlay"))
        )

    def no_accounts_exist(self):
        elements = self.driver.find_elements_by_xpath(
            "//div[@class='error' and contains(text(), 'No Account(s)/SA(s) exists for the given criteria')]"
        )
        return len(elements) > 0

    def retail_customer_identifier(self):
        pattern = r"https://sharemydata.pge.com/myAuthorization/auth/manage/(\d+)"

        results = re.findall(pattern, self.driver.current_url)

        if len(results) == 1:
            return results[0]

        log.error(
            "Failed to determine a unique retail customer identifier. Results: %s",
            results,
        )
        return None

    @classmethod
    def parse_account_label(cls, label):
        """Process account name, account number, and green button customer account identifier from the label."""

        # The account labels we expect to see look like this:
        # 'HANFORD COMMUNITY HOSPITAL - Account # : 1803851256 - Account UUID: 8948315819'
        # 'CALLINAN, THOMAS W - Account  # : 6225981787 - 120 Capp Elec Account UUID: 4183315816'

        match = re.match(r"(.*) - Account # : (.+) Account UUID: (\d+)", label)

        if not match:
            template = "Could not register Account Name, Number, and Green Button ID from the label: %s."
            raise AccountParseFailure(template % label)
        return match.group(1), match.group(2), match.group(3)

    def scrape(self):
        accounts = self.driver.find_elements_by_xpath('//*[@id="accordion"]/div[2]/div')

        def process(dom_element, xpath):
            text = dom_element.find_element_by_xpath(xpath).get_attribute("innerText")
            return text.replace("-", "").strip()

        accumulator = []
        for acct in accounts:
            account_span = acct.find_element_by_xpath(
                ".//div/div[1]/span[2]/label/span"
            )
            account_label = account_span.get_attribute("innerText")
            acct_name, acct_number, acct_uuid = self.parse_account_label(account_label)

            acct_rows = acct.find_elements_by_xpath(
                './/*[@id="collapse2"]/div/table/tbody/tr'
            )

            meters = []
            for row in acct_rows:

                if len(row.find_elements_by_xpath("./td")) < 6:
                    # Sometimes PG&E injects dummy rows of just comments like "<!---->", which we need to skip.
                    continue

                address = process(row, "./td[3]/label")
                service = process(row, "./td[4]")
                usage_point = process(row, "./td[5]")
                service_type = process(row, "./td[6]").lower()

                meters.append(
                    MeterSummary(
                        address=address,
                        service_id=service,
                        service_uuid=usage_point,
                        service_type=service_type,
                    )
                )

            accumulator.append(
                AccountSummary(
                    name=acct_name,
                    account_number=acct_number,
                    account_uuid=acct_uuid,
                    meters=meters,
                )
            )

        return accumulator

    def authorize(self):
        """Press the 'submit' button to trigger authorization."""
        submit_button = self.driver.find_element_by_id("smd-submit")
        submit_button.click()

        log.info("Clicked the submit button for authorization.")

        # Make sure the spinner is gone.
        WebDriverWait(self.driver, 120).until(
            ec.invisibility_of_element_located((By.CLASS_NAME, "overlay"))
        )

        # We need to wait for our side of the oauth to load.
        # The "success" page just has one button on it, labelled continue.
        WebDriverWait(self.driver, 120).until(
            ec.element_to_be_clickable((By.XPATH, "//button"))
        )
        log.info("Clicked continue button on Gridium-side oauth page.")

        # Wait five seconds for any remaining processing that might be needed.
        time.sleep(5)

        log.info("Authorization complete.")


def with_selenium(fn):
    """Wrap the input procedure with code to manage selenium and virtual display lifecycles."""

    def run_under_selenium(*args, **kwargs):
        display = VirtualDisplay()
        if config.USE_VIRTUAL_DISPLAY:
            display.start()

        driver = webdriver.Chrome()
        try:
            return fn(driver, *args, **kwargs)
        finally:
            driver.quit()
            if config.USE_VIRTUAL_DISPLAY:
                display.stop()

    return run_under_selenium


def _authorize(driver, username, password, provider, verify=False, dryrun=False):
    """Given PG&E credentials, log in and authorize CE to receive Green Button data."""
    login_page = LoginPage(driver)
    login_page.login(username, password)

    if login_page.login_successful():
        log.info("Login succeeded.")
    else:
        raise LoginFailure("Authorization failed: Invalid credentials.")

    home = HomeScreen(driver)
    home.wait_until_ready()

    found_authorized = False
    if home.is_authorized(provider):
        log.info("%s is authorized. Inspecting meter-level authorizations.", provider)
        auth_page = home.browse_to_authorizations(provider)
        found_authorized = True

    else:
        log.info("Determined %s does not have a recent authorization.", provider)
        home.select_provider(provider)
        auth_page = home.browse_to_auth_config_page()

    auth_page.wait_until_ready()
    if auth_page.no_accounts_exist():
        raise NoAccountsFailure("No Account(s)/SA(s) associated with this login.")

    # Regardless of whether we have authorized, the auth page will show us the Green Button Customer Account
    # and Green Button Usage Point identifiers for each meter.
    summaries = auth_page.scrape()
    log.info(
        "Obtained %s account summaries covering %s meters.",
        len(summaries),
        sum(len(a.meters) for a in summaries),
    )

    subscription_id = None
    if verify and found_authorized:
        subscription_id = auth_page.retail_customer_identifier()

    if not (verify or found_authorized or dryrun):
        auth_page.authorize()

    return AuthorizationSummary(
        found_authorized=found_authorized,
        accounts=summaries,
        subscription_id=subscription_id,
    )


# Pylint gets confused by signature changes caused by decorators, so we do this directly here instead.
authorize = with_selenium(_authorize)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape PGE.com Share My Data to determine proper SMD Authorizations."
    )
    subparser = parser.add_subparsers(dest="command")
    subparser.required = True

    authorize_sp = subparser.add_parser("authorize")
    authorize_sp.add_argument("provider", help="CE or Gridium")
    authorize_sp.add_argument("username", help="PGE.com Username")
    authorize_sp.add_argument("password", help="PGE.com Password")
    authorize_sp.add_argument(
        "--verify",
        help="Check if we authorized and collect the subscription ID.",
        action="store_true",
    )
    authorize_sp.add_argument(
        "--dryrun",
        help="Run all authorization steps except triggering the final authorization.",
        action="store_true",
    )

    deauthorize_sp = subparser.add_parser("deauthorize")
    deauthorize_sp.add_argument("provider", help="CE or Gridium")
    deauthorize_sp.add_argument("username", help="PGE.com Username")
    deauthorize_sp.add_argument("password", help="PGE.com Password")
    deauthorize_sp.add_argument(
        "--dryrun",
        help="Run all deauthorization steps except removing the authorization.",
        action="store_true",
    )

    log.info("Starting authorization scraper run.")
    args = parser.parse_args()

    provider = args.provider.lower()

    if args.command == "authorize":
        summary = authorize(
            args.username,
            args.password,
            provider,
            verify=args.verify,
            dryrun=args.dryrun,
        )
        log.info("Final Data Summary: %s", pprint.pformat(summary.to_json()))

    log.info("Done.")
    sys.exit(0)


if __name__ == "__main__":
    log.setLevel(logging.INFO)
    main()
