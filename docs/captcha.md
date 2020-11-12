# Captcha solving

If a site adds a captcha, we can send the image to be solved via [2captcha.com](https://2captcha.com/2captcha-api).

## how to use it in a scraper

```
from datafeeds.common.captcha import recaptcha_v2

recaptcha_v2(
    # Selenium web driver
    self.driver,
    # WebElement that contains the recaptcha iframe
    self.driver.find_element_by_id("datadownload-content"),
    # url of the page containing the captcha
    "https://www.sce.com/sma/ESCAA/EscGreenButtonData#viewDDForParticularAccount",
)
```

See [SceEnergyManagerGreenButtonDownload](../datafeeds/scrapers/sce_react/pages.py) scraper for an example.

## how it works

The [AWS Batch job definition for datafeeds](https://us-west-1.console.aws.amazon.com/batch/v2/home?region=us-west-1#job-definition)
contains `CAPTCHA_API_KEY` as an environment variable. The API key is available from the 2captcha dashboard; the
credentials are in LastPass.

To run locally, add `export CAPTCHA_API_KEY="key_here"` to your local environment before starting the scraper.

2captcha's API docs: https://2captcha.com/2captcha-api#solving_captchas

To solve a ReCaptcha v2:

  - get the key (`k` url parameter) from the captcha iframe
  - get cookies from the browser
  - send a request to https://2captcha.com/in.php with the API key, captcha key, page URL, and cookies; save the returned `request` parameter
  - wait 30 seconds, then call https://2captcha.com/res.php with `request` parameter
  - if this returns an error, try up to 5 more times, 10 seconds apart
  - otherwise, set the `g-recaptcha-response` element in the browser to the response; this should solve the captcha

See [../datafeeds/common/catpcha.py](datafeeds/common/captcha.py) for the full code.
