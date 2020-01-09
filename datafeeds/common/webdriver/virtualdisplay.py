"""
Simple wrapper class for pyvirtualdisplay to standardize init options
"""
import logging
import pyvirtualdisplay

from datafeeds import config

log = logging.getLogger("datafeeds")


class VirtualDisplay:
    def __init__(self):
        self._display = pyvirtualdisplay.Display(
            visible=config.DEBUG_SELENIUM_SCRAPERS, size=(1900, 1200)
        )

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    def start(self):
        log.info("Starting virtual display")
        self._display.start()

    def stop(self):
        log.info("Stopping virtual display")
        self._display.stop()
