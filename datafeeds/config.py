import logging.config
import os
from os import path


DATAFEEDS_ROOT = path.normpath(path.join(path.dirname(path.abspath(__file__)), ".."))
DATAFEEDS_LOG_NAME = os.environ.get("DATAFEEDS_LOG_NAME", "datafeeds.log")
DATAFEEDS_ENVIRONMENT = os.environ.get("DATAFEEDS_ENVIRONMENT", "local").lower()  # or development, production
UNDER_TEST: bool = (os.environ.get("DATAFEEDS_UNDER_TEST", "False").lower() == "true")

WORKING_DIRECTORY: str = os.environ.get("WORKING_DIRECTORY", path.join(DATAFEEDS_ROOT, "workdir"))

POSTGRES_URL: str = os.environ.get("POSTGRES_URL", "postgresql+psycopg2://postgres@pg/gridium_test")
POSTGRES_ECHO: bool = (os.environ.get("POSTGRES_ECHO", "False").lower() == "true")

ARCHIVE_S3_BUCKET: str = os.environ.get("ARCHIVE_S3_BUCKET", "gridium-dev-datafeeds-archive")
UPLOAD_ARCHIVES: bool = os.environ.get("UPLOAD_ARCHIVES", "False").lower() == "true"

URJANET_MYSQL_HOST: str = os.environ.get("URJANET_MYSQL_HOST")
URJANET_MYSQL_USER: str = os.environ.get("URJANET_MYSQL_USER")
URJANET_MYSQL_PASSWORD: str = os.environ.get("URJANET_MYSQL_PASSWORD")
URJANET_MYSQL_DB: str = os.environ.get("URJANET_MYSQL_DB")

ELASTICSEARCH_HOSTS: str = os.environ.get("ETL_ELASTICSEARCH_HOSTS")
ELASTICSEARCH_AUTH: str = os.environ.get("ETL_ELASTICSEARCH_ATUH")
ELASTICSEARCH_SSL: bool = os.environ.get("ETL_ELASTICSEARCH_SSL", "False").lower() == "true"

WEBAPPS_DOMAIN: str = os.environ.get("WEBAPPS_DOMAIN")
WEBAPPS_TOKEN: str = os.environ.get("WEBAPPS_TOKEN")

AES_KEY: str = os.environ.get("AES_KEY")

PLATFORM_API_URL: str = os.environ.get("PLATFORM_API_URL")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
DEPENDENCY_LOG_LEVEL = os.environ.get("DEPENDENCY_LOG_LEVEL", "WARN")
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
    # log warning+ for non-Gridium libs
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
