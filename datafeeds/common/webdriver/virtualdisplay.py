"""
Simple wrapper class for pyvirtualdisplay to standardize init options
"""
import logging

from xvfbwrapper import Xvfb

log = logging.getLogger("datafeeds")


class VirtualDisplay:
    def __init__(self):
        self._display = Xvfb(width=1900, height=1200)

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
