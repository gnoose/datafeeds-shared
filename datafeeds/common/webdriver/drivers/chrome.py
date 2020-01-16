import os

from selenium import webdriver

from datafeeds import config
from datafeeds.common.webdriver.drivers.base import BaseDriver


class ChromeDriver(BaseDriver):
    def __init__(self, outputpath):
        super().__init__(outputpath)
        self._driver = webdriver.Chrome(
            chrome_options=self._options(),
            service_log_path=os.path.join(config.WORKING_DIRECTORY, "driver.log"),
        )

    def _options(self):
        options = webdriver.ChromeOptions()

        # Cannot seem to use .add_argument anymore - for some reason api changed
        # https://stackoverflow.com/questions/35331854/downloading-a-file-at-a-specified-location-through-python-and-selenium-using-chr/35333535#answer-43789674
        prefs = {}
        prefs["download.default_directory"] = self.download_dir
        prefs["plugins.always_open_pdf_externally"] = True
        prefs["profile.default_content_setting_values.automatic_downloads"] = 1
        # Disable the PDF viewer so that PDFs are downloaded
        prefs["plugins.plugins_list"] = [
            {"enabled": False, "name": "Chrome PDF Viewer"}
        ]

        options.add_experimental_option("prefs", prefs)
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        return options
