# Working with a Cloud9 environment

[Cloud9 User Guide](https://docs.aws.amazon.com/cloud9/latest/user-guide/welcome.html)

Login to `gridium-dev` AWS account with IAM credentials, and go to the
[Cloud9 home page](https://console.aws.amazon.com/cloud9/home).

Click Open IDE to start the environment.

## terminal setup

To run scrapers from a terminal, run this first:

```
pyenv activate datafeeds
cd datafeeds
scripts/start_chrome.sh
```

## run a scraper

Run launch.py with the datasource id and a date range:

    python launch.py by-oid *datasource* 2021-03-01 2021-03-07

Outputs (screenshots, downloaded files, data) are in `workdir`. This is cleared at the start of every run.

To view screenshots, open them from the project sidebar.

## get credentials

    python scripts/get_credentials.py *datasource*
