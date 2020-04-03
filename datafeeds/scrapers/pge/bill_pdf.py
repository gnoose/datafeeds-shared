import logging
from datetime import date
from typing import List, Optional

from datafeeds.common.base import BaseWebScraper, CSSSelectorBasePageObject
from datafeeds.common.batch import run_datafeed
from datafeeds.common.support import Results, Configuration
from datafeeds.common.typing import Status, BillPdf

# from datafeeds.common.upload import hash_bill
from datafeeds.common.upload import upload_bill_to_s3
from datafeeds.models import (
    SnapmeterAccount,
    Meter,
    SnapmeterMeterDataSource as MeterDataSource,
)

log = logging.getLogger(__name__)


class DashboardPage(CSSSelectorBasePageObject):
    """Dashboard page with account dropdown.

    https://m.pge.com/index.html#myaccount/dashboard/summary/account-id
    """

    def select_account(self, account_id: str):
        """Select account from dropdown.

        Account number is 10 digits. The dropdown includes an additional check digit: 1234567890-5
        """
        pass

    def download_bills(self, start_date: date, end_date: date) -> List[str]:
        """Download bill PDFs for the specified date range."""
        keys: List[str] = []
        """
        click Bill & Payment history arrow
        click View up to 24 months of activity
        if date not in range, skip
        click View Bill PDF
          - file downloads to config.WORKING_DIRECTORY
          - get filename: last 4 digits of account (excluding check digit), then date (03262020)
            - example: 0440custbill03262020.pdf
          - create key: hash_bill(self.utility_account, start_date, end_date, cost)
          - upload with upload_bill_to_s3(file_handle: BytesLikeObject, key: str)
          - add key to list
        """
        return keys


class LoginPage(CSSSelectorBasePageObject):
    UsernameFieldSelector = 'input[name="username"]'
    PasswordFieldSelector = 'input[name="password]'
    SigninButtonSelector = 'button[id="home_login_submit"]'

    def login(self, username: str, password: str):
        """Authenticate with the web page.

        https://www.pge.com/
        Fill in the username, password, then click "Sign in"
        """
        pass


# old Java implementation for reference
"""
    private final static String PGE_URL = "https://www.pge.com";

    private Problem problem = Problem.ok;

    @Override
    public void execute(Tuple session)
    {
        WebDriver driver = session.at(WebDriver.class, "web-driver");
        ProvisionAssignment assignment = session.at(ProvisionAssignment.class, "assignment");

        // The visible page will load well before "driver.get" completes,
        // since it waits on all extra scripts to load and there are a LOT
        // of extras, like ad launchers, etc
        debug("Navigating to PG&E");
        driver.get(PGE_URL);
        new WebDriverWait(driver, 60)
            .until(presenceOfElementLocated(By.name("username")));

        try
        {
            debug("Filling in form");
            logIn(driver, assignment);
            debug("Successfully logged in");
            saveScreen(session, driver, "after-login");
        }
        catch (TimeoutException toEx)
        {
            if (showsOutage(driver))
            {
                debug("Shows outage page");
                clickThroughOutage(driver);
            }
            else
            {
                updateProblem(driver);
                saveScreen(session, driver, "failed-login");
                assignment.wrongCredentials();
                session.put(nv("etl-status", ETLStatus.credentials));
            }
        }
    }

    /**
     * Fill in and submit login form
     */
    private void logIn(WebDriver driver, ProvisionAssignment assignment)
    {
        ProvisionOrigin origin = assignment.origin();
        EncryptedContent user = origin.user();
        EncryptedContent pass = origin.pass();

        driver.findElement(By.name("username")).sendKeys(user.value());
        driver.findElement(By.name("password")).sendKeys(pass.value());
        driver.findElement(By.id("home_login_submit")).click();
        waitForAccount(driver);
    }

    /**
     * Failed logins will display a message of some sort
     */
    private boolean hasLoginError(WebDriver driver, String text)
    {
        String sel = "//p[@class='login-error-msg' and contains(text(), '" + text + "')]";
        return driver.findElements(By.xpath(sel)).size() > 0;
    }

    /**
     * Some logins will actually not have any accounts linked at all
     * and do not show any available
     */
    private boolean hasNoAccounts(WebDriver driver)
    {
        By byText = By.xpath(
            "//div[contains(text(), " +
            "'You do not have any accounts linked to your username')]");
        By byForm = By.xpath("//input[@id='cyai-accountNumber']");

        // Look for both "no accounts" message as well as account form,
        // because there's a chance (like on the account linking page)
        // there *could* be accounts and might not show the form
        return driver.findElements(byText).size() > 0 &&
            driver.findElements(byForm).size() > 0;
    }

    /**
     * Sometimes a successful login is redirected to an "outage" page
     */
    private boolean showsOutage(WebDriver driver)
    {
        By byOutageHeader = By.xpath("//h1[contains(@class, 'pgeOutage')]");
        return driver.findElements(byOutageHeader).size() > 0;
    }

    /**
     * Goes from "outage" page to main account dashboard
     */
    private void clickThroughOutage(WebDriver driver)
    {
        driver.findElement(By.cssSelector(
            "a[title='Residential - Your Account']"
        )).click();
        driver.findElement(By.cssSelector(
            "a[title='Account Overview - View Your Account Dashboard']"
        )).click();
        waitForAccount(driver);
    }

    /**
     * Main account homepage after login
     */
    private void waitForAccount(WebDriver driver)
    {
        new WebDriverWait(driver, 120)
            .until(presenceOfElementLocated(By.id("accountUserName")));

        // The overlay will flicker several times before the page is fully loaded,
        // so try to sleep through the first few series of flickers
        sleep(5);
        waitForMainOverlay(driver, 120);
    }

    /**
     * There are a few (known) possible problems with logging in, so let's
     * disambiguate the login error a little bit...
     */
    private void updateProblem(WebDriver driver)
    {
        String msg = "Couldn't log in";

        // Note: These are the actual error strings returned, so they are case-sensitive
        String accountDisabled = "Account temporarily disabled";
        String invalidCredentials = "Invalid Username or Password";

        if (hasLoginError(driver, accountDisabled))
        {
            msg = accountDisabled;
        }
        else if (hasLoginError(driver, invalidCredentials))
        {
            msg = invalidCredentials;
        }
        else if (hasNoAccounts(driver))
        {
            msg = "No accounts for login";
        }

        problem = problem.put(msg);
    }

        """


# old Java implementation to extract bills
"""
    /**
     * Visits dashboard, retrieves all accounts from Sonar,
     * then for each account (1) loads services from Sonar
     * and (2) performs the provided step
     */
    @Override
    public void execute(Tuple session)
    {
        WebDriver driver = session.at(WebDriver.class, "web-driver");
        CESonar sonar = session.at(CESonar.class, "sonar");

        debug("Visiting main dashboard");

        try
        {
            visitDashboard(driver);
        }
        catch (Exception ex)
        {
            session.put(nv("etl-status", ETLStatus.nothing));
            problem = problemWithTrace(ex, "Error loading main dashboard");
            return;
        }

        List<PGEAccount> accounts = getAccounts(driver, sonar);
        debug("Found " + accounts.size() + " accounts");

        ETLStatus etlStatus = null;

        for (PGEAccount account : accounts)
        {
            List<PGEService> services = sonar.findServices(account);

            try
            {
                debug(2, "Visiting account summary for " + account.account());
                loadAccount(driver, account);
            }
            catch (Exception ex)
            {
                Problem p = problemWithTrace(ex, "Failed to load account " + account.account());

                for (PGEService service : services) {
                    sonar.findAssignmentPart(service).failed(p);
                }

                etlStatus = ETLStatus.partial;
                continue;
            }

            // Let performStep handle problems on individual service basis
            renewSessionIfNecessary(driver);

            try
            {
                performStep(session, sonar, driver, account, services);
            }
            catch (Exception ex)
            {
                debug(2, "ERROR: could not perform step for account " + account.account());
                debug(ex.getMessage());
            }
        }

        if (etlStatus == null)
        {
            etlStatus = ETLStatus.complete;
        }
        session.put(nv("etl-status", etlStatus));
    }

    /**
     * Return to main page and wait for accounts to load
     */
    private void visitDashboard(WebDriver driver)
    {
        driver.get("https://m.pge.com/#dashboard");
        new WebDriverWait(driver, 120)
            .until(presenceOfElementLocated(By.id("accountListItems")));
        waitForMainOverlay(driver, 30);
    }

    /**
     * Visit dashboard for a given account directly
     */
    private void loadAccount(WebDriver driver, PGEAccount account)
    {
        driver.get("https://m.pge.com/#myaccount/dashboard/summary/" + account.account());
        new WebDriverWait(driver, 120)
            .until(presenceOfElementLocated(By.className("NDB-footer-links")));
        waitForMainOverlay(driver, 30);
    }

    /**
     * Get available account IDs in dropdown selector and retrieve via sonar
     */
    private List<PGEAccount> getAccounts(WebDriver driver, CESonar sonar)
    {
        List<PGEAccount> accounts = new ArrayList<>();

        // Grab number of accounts and then iterate using index, as storing references
        // to DOM elements will encourage Stale Element Errors
        int accountsCount = driver.findElements(By.className("accountListItem")).size();

        // xpath is 1-based but dropdown has extra first row for "view accounts"
        for (int row = 2; row <= accountsCount + 1; row++)
        {
            String accountNumber = driver
                .findElement(By.xpath("//ul[@id='accountListItems']//li[" + row + "]//a"))
                // For some reason, .getText() returns "" but get..(innerText) returns the account number,
                // even though there is no inner node
                .getAttribute("innerText").trim();

            // There is a chance that the dropdown might show account names as well,
            // so ONLY grab number w/ check digit
            if (accountNumber.length() > 12)
            {
                accountNumber = accountNumber.substring(0, 12);
            }

            accounts.add(sonar.findAccount(accountNumber));
        }

        return accounts;
    }


    private static By BY_BILLING_TABLE_SEL =
        By.xpath("//div[@id='billingHistoryContainer']");

    private static String PANEL_XSEL = "//div[" +
            "contains(@class, 'billed_history_panel') and " +
            "contains(@class, 'pge_coc-dashboard-viewPay_billed_summary_panel') and " +
            "not(contains(@class, 'hide'))]";

    private Pattern headerCostPattern = Pattern.compile("\\s\\$([\\d,\\.]+)\\s");


    void performStep(Tuple session, CESonar sonar, WebDriver driver,
                     PGEAccount account, List<PGEService> services)
    {

        // FIXME kvlr
        // This is weird, but... the overlay is *gone* at this point and we can do
        // 10 "waitForMainOverlay" iterations and they will finish immediately because
        // it never shows up and is always "invisible", but the moment we try to do this
        // click the overlay blocks it...?
        try
        {
            openBillingHistory(driver);
        }
        catch (Exception ex)
        {
            sleep(5);
            openBillingHistory(driver);
        }
        viewFullHistory(driver);

        // Rather than get all matching elements and iterate through, use index
        // and manually get element each time to help avoid stale element errors
        int count = driver
            .findElements(By.xpath(PANEL_XSEL))
            .size();

        debug(2, "Found " + count + " panels in billing widget");

        // xpath indices are 1-based
        for (int i = 1; i <= count; i++)
        {
            String baseSel = PANEL_XSEL + "[" + i + "]";
            By byHeaderSel = By.xpath(baseSel + "//*[contains(@class, 'panel-title')]");

            String headerText = driver.findElement(byHeaderSel).getText();

            if (headerText.contains("Payment"))
            {
                debug(3, "Skipping panel " + i + " (payment)");
                continue;
            }

            debug(3, "Processing panel " + i + " (bill)");

            // Get date from the "data-date" attribute on link to download bill...
            By byLinkSel = By.xpath(baseSel +
                "//div[contains(@class, 'pge_coc-dashboard-viewPay_billed_history_panel_viewBill_para_block')]" +
                "//a[contains(@class, 'viewBill')]");

            String timestamp = driver.findElement(byLinkSel).getAttribute("data-date");

            java.util.Date crappyDate = new java.util.Date(Long.parseLong(timestamp));
            Date billDate = Date.from(crappyDate);

            PGEBill bill = sonar.findBill(account, billDate);

            // Each header will have two numbers - cost and balance - so we want
            // to extract the first one and convert to double
            Matcher m = headerCostPattern.matcher(headerText);

            if (m.find())
            {
                double cost = Double.parseDouble(m.group(1).replace(",", ""));
                bill.cost(cost);
            }

            debug(3, "Found bill for " + bill.date().toString() + " with cost $" + bill.cost());

            try
            {
                click(driver, byLinkSel);
            }
            catch (ElementNotVisibleException ex)
            {
                debug(2, "Download link not visible; looking for other");
                click(driver, By.xpath(baseSel +
                    "//div[@id='billSummaryContainer']" +
                    "//a[contains(@class, 'viewBill')]"));
            }

            transferBill(session, driver, bill);
        }
    }

    /**
     * Waits for bill to download, transfers to S3, and stores key on PGEBill object
     */
    void transferBill(Tuple session, WebDriver driver, PGEBill bill)
    {
        PGEProvisionConfiguration config = session.at(PGEProvisionConfiguration.class, "config");
        Path outputpath = session.at(Path.class, "outputpath");

        // FIXME kvlr: is there a way to KNOW the file being downloaded,
        // rather than making assumptions about filename?
        //
        // Filename will be like "1234custbill08302018.pdf", where "1234"
        // are the last 4 digits of account number (excluding check digit)
        String last4 = bill.account().account().substring(6, 10);
        String date = left(bill.date().month(), 2) + left(bill.date().day(), 2) + bill.date().year();
        String filename = last4 + "custbill" + date + ".pdf";

        // Catch on upload attempt because if something goes wrong it would better
        // to have a record of a bill without an upload than no record of bill at all
        File pdf;

        try
        {
            pdf = new WebDriverWait(driver, 30).until(webDriver -> {
                File expected = new File(outputpath.toString() + "/" + filename);

                return expected.exists() ? expected : null;
            });
        }
        catch (TimeoutException ex)
        {
            debug(3, "ERROR waiting for file " + filename + " to download");
            return;
        }

        // The bills download currently tries to de-dupe accounts, meaning they are not delivered
        // scoped to login at all, so to facilitate that only use account number/date...
        // Consequence of this is that logins that share accounts/bills will all share a set
        // of PDF files even though they will have different PGEBill records
        String key = bill.account().account() + "." + date + ".pdf";
        S3Client s3 = new S3Client(config.bucket());

        try
        {
            // These PDFs need neither high uptime nor high durability, since we can re-scrape
            // at any time to replace them if the single zone data center dies in an earthquake
            s3.push(key, pdf, Optional.of(StorageClass.OneZoneInfrequentAccess));
            s3.waitForCompletion();
        }
        catch (Exception ex)
        {
            debug(3, "ERROR uploading bill for " + key);
            debug(ex.getMessage());
            return;
        }

        bill.key(key);
        debug(3, "Uploaded " + filename + " to " + key);
    }

    /**
     * Clicks the "expand" arrow to open up Bill & Payment history,
     * waiting until table is loaded
     */
    void openBillingHistory(WebDriver driver)
    {
        debug(2, "Opening billing history");
        click(driver, By.xpath(
            "//div[@id='arrowBillPaymentHistory']" +
            "//div[contains(@class, 'pge_coc-dashboard-panel_title_arrow_icon')]"));

        waitForBillingTable(driver);
    }

    /**
     * Clicks the "View up to 24 months.." link and waits for table to load
     */
    void viewFullHistory(WebDriver driver)
    {
        debug(2, "Clicking 'view up to..' link");

        // As always, cannot scope only by @id since multiple elements share id
        click(driver, By.xpath(
            "//div[@class='pge_coc-dashboard-viewPay_billing_hist_panel_gp']" +
            "//a[@id='href-view-24month-history']"));

        // Table is already present in DOM but will disappear after clicking,
        // so wait for that BEFORE waiting for it to reappear
        new WebDriverWait(driver, 5)
            .until(invisibilityOfElementLocated(BY_BILLING_TABLE_SEL));

        waitForBillingTable(driver);
    }

    /**
     * Waits for billing history table to load
     */
    void waitForBillingTable(WebDriver driver)
    {
        new WebDriverWait(driver, 60)
            .until(presenceOfElementLocated(BY_BILLING_TABLE_SEL));
    }
"""


class PgeBillPdfScraper(BaseWebScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.browser_name = "Chrome"
        self.name = "PGE Bill PDF"
        self.login_url = "https://www.pge.com"

    def _execute(self):
        # Direct the driver to the login page
        self._driver.get(self.login_url)

        # Create page helpers
        login_page = LoginPage(self._driver)
        dashboard_page = DashboardPage(self._driver)

        # Log in
        login_page.wait_until_ready(login_page.UsernameFieldSelector)
        self.screenshot("before login")
        login_page.login(self.username, self.password)
        self.screenshot("after login")
        # TODO: wait until page ready

        # select account
        dashboard_page.select_account(self._configuration.utility_account)
        self.screenshot("after select account")
        # TODO: wait until account loads

        # download bills
        pdfs = []
        for key in dashboard_page.download_bills(self.start_date, self.end_date):
            pdfs.append(
                BillPdf(
                    utility_account_id=self._configuration.utility_account,
                    start=self.start_date,
                    end=self.end_date,
                    s3_key=key,
                )
            )
        return Results(pdfs=pdfs)


class PgeBillPdfConfiguration(Configuration):
    def __init__(self, utility_account: str):
        super().__init__(scrape_pdfs=True)
        self.utility_account = utility_account


def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    configuration = PgeBillPdfConfiguration(
        utility_account=meter.utility_service.utility_account_id,
    )

    return run_datafeed(
        PgeBillPdfScraper,
        account,
        meter,
        datasource,
        params,
        configuration=configuration,
        task_id=task_id,
    )
