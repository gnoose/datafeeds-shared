# Scraper fixing cookbook

Add best practices here.

## see the scraper run

  - run `scripts/start_chrome.sh` to start Selenium Chrome with a VNC server
  - in the IDE, click Preview, Preview Running Application ; this will open the application in an IDE panel
  - click Connect, then enter `secret` as the password
  - run the scraper from the command line (`python launch.py by-oid 123 2021-01-01 2021-04-01`)
  - to open Selenium in a new tab (outside the IDE), click Pop Out Into New Window (top right)

## screenshots

In a scraper class (derived from `BaseWebScraper`):

    self.screenshot("description")

In a page object (derviced from `PageState`):

    from datafeeds.common.base import BaseWebScraper
    self.driver.screenshot(BaseWebScraper.screenshot_path("description"))

Screenshots are written to `png` files in `workdir`.


## credentials

Create a datasource in the database, for use by scrapers:

    python scripts/create_test_data_source.py scraper username password --service_id 123 --utility_account_id 456

Get credentials, for use in local browser (replace 456 with the datasource id for the current project)

    python scripts/get_credentials.py 456
