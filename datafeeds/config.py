import logging.config
import os
from os import path


from typing import Set

# Where is the datafeeds project source code on disk?
DATAFEEDS_ROOT = path.normpath(path.join(path.dirname(path.abspath(__file__)), ".."))

# What shall the datafeeds log be called?
DATAFEEDS_LOG_NAME = os.environ.get("DATAFEEDS_LOG_NAME", "datafeeds.log")

# What is the name of the environment in which datafeeds is running? Ex. local, development, production
DATAFEEDS_ENVIRONMENT = os.environ.get("DATAFEEDS_ENVIRONMENT", "local").lower()

# Is datafeeds running in a unit test framework?
# (Used to simplify DB transaction management and rollback commits.)
UNDER_TEST: bool = (os.environ.get("DATAFEEDS_UNDER_TEST", "True").lower() == "true")

# What directory should be used for storing scraper artifacts like screenshots and downloads?
WORKING_DIRECTORY: str = os.environ.get("WORKING_DIRECTORY", path.join(DATAFEEDS_ROOT, "workdir"))

# What is the full URL needed to log into the PostgreSQL instance? (Hostname, username, password, and dbname)
POSTGRES_URL: str = os.environ.get("POSTGRES_URL", "postgresql+psycopg2://postgres@pg/gridium_test")

# Should every SQL query run by datafeeds be echoed to the console?
POSTGRES_ECHO: bool = (os.environ.get("POSTGRES_ECHO", "False").lower() == "true")

# Which S3 bucket should store bill pdfs acquired during the scraper process?
# (Webapps and datafeeds share access to this resource.)
BILL_PDF_S3_BUCKET = os.environ.get("BILL_PDF_S3_BUCKET")

# Which S3 bucket should store the compressed working directory of artifacts for each scraper run?
ARTIFACT_S3_BUCKET: str = os.environ.get("ARTIFACT_S3_BUCKET", "gridium-dev-datafeeds-archive")

# What are the network details and credentials needed to connect to the urjanet MySQL database?
URJANET_MYSQL_HOST: str = os.environ.get("URJANET_MYSQL_HOST", "urjanet")
URJANET_MYSQL_USER: str = os.environ.get("URJANET_MYSQL_USER", "gridium")
URJANET_MYSQL_PASSWORD: str = os.environ.get("URJANET_MYSQL_PASSWORD", "gridium")
URJANET_MYSQL_DB: str = os.environ.get("URJANET_MYSQL_DB", "urjanet")

# What are the API credentials for gridium's Urjanet account?
URJANET_HTTP_USER: str = os.environ.get("URJANET_HTTP_USER")
URJANET_HTTP_PASSWORD: str = os.environ.get("URJANET_HTTP_PASSWORD")

# Which Elasticsearch host should receive index details about running scraper jobs?
ELASTICSEARCH_HOST: str = os.environ.get("ELASTICSEARCH_HOST")
ELASTICSEARCH_PORT: int = int(os.environ.get("ELASTICSEARCH_PORT", "9200"))
ELASTICSEARCH_USER: str = os.environ.get("ELASTICSEARCH_USER")
ELASTICSEARCH_PASSWORD: str = os.environ.get("ELASTICSEARCH_PASSWORD")

# How does datafeeds connect to webapps?
WEBAPPS_DOMAIN: str = os.environ.get("WEBAPPS_DOMAIN")
WEBAPPS_TOKEN: str = os.environ.get("WEBAPPS_TOKEN")

# What key shall datafeeds use to decrypt credentials stored in postgres?
AES_KEY: str = os.environ.get("AES_KEY")

# How does datafeeds connect to platform?
PLATFORM_HOST: str = os.environ.get("PLATFORM_HOST")
PLATFORM_PORT: str = os.environ.get("PLATFORM_PORT", "9229")


# Where can we find the Ingest REST API?
INGEST_ENDPOINT: str = os.environ.get("INGEST_ENDPOINT")

#
# What features are enabled?
# S3_ARTIFACT_UPLOAD: After each scraper run, datafeeds should upload a compressed archive of log data,
#                     screenshots, etc. to S3.
# S3_BILL_UPLOAD: As bill PDFs are discovered by scrapers, they will be uploaded to an S3 bucket for consumption
#                 via the energy-analytics UI.
# PLATFORM_UPLOAD: After a scraper run, scrapers will upload their interval/bill data to platform.
# ES_INDEX_JOBS: As part of the scraping process, we will upload current task to elasticsearch for use in dashboards.
#
VALID_FEATURE_FLAGS: Set[str] = {"S3_ARTIFACT_UPLOAD", "S3_BILL_UPLOAD", "PLATFORM_UPLOAD", "ES_INDEX_JOBS"}
FEATURE_FLAGS: Set[str] = \
    set(u.strip().upper() for u in os.environ.get("FEATURE_FLAGS", "").split(",")) & VALID_FEATURE_FLAGS


def enabled(feature: str) -> bool:
    if feature not in VALID_FEATURE_FLAGS:
        raise Exception("%s is not a valid feature flag. Add it to VALID_FEATURE_FLAGS in the config module.")
    return feature in FEATURE_FLAGS


# What log level should datafeeds' logger use?
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# What log level shall datafeeds dependencies use?
DEPENDENCY_LOG_LEVEL = os.environ.get("DEPENDENCY_LOG_LEVEL", "WARN")

# Are we running the web scraper in headless mode?
USE_VIRTUAL_DISPLAY = os.environ.get("USE_VIRTUAL_DISPLAY", False)

SLACK_TOKEN = os.environ.get("SLACK_TOKEN", None)

DEBUG_SELENIUM_SCRAPERS = 0

# Default selenium browser
SELENIUM_BROWSER = "Chrome"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {
            "format": "%(asctime)s : %(levelname)s : %(message)s",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard"
        },
        "file": {
            "class": "logging.FileHandler",
            "filename": DATAFEEDS_LOG_NAME,
            "formatter": "standard"
        }
    },
    "root": {
        "handlers": ["console"],
        "level": DEPENDENCY_LOG_LEVEL,
        "propagate": False
    },
    "loggers": {
        "datafeeds": {
            "level": LOG_LEVEL,
            "handlers": ["console"] if DATAFEEDS_ENVIRONMENT == "local" else ["console", "file"],
            "propagate": False
        },
    }
}

logging.config.dictConfig(LOGGING)
