# Scraper fixing cookbook

Add best practices here.

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

Get credentials, for use in local browser

    python scripts/get_credentials 456
