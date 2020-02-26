from unittest import TestCase
import logging

from datafeeds import config
from datafeeds.common.test_utils import private_fixture, FixtureNotFoundError


log = logging.getLogger(__name__)


class ExamplePrivateFixtureTest(TestCase):
    def test_private_fixture(self):
        if not config.TEST_WITH_PRIVATE_FIXTURES:
            log.info("Skipping test, private fixtures not enabled.")
            return

        fixture = private_fixture("dummy_secret.txt").strip()
        expected = b"This is not a real secret, just an example for testing."
        self.assertEqual(expected, fixture)

    def test_private_fixture_not_found(self):
        if not config.TEST_WITH_PRIVATE_FIXTURES:
            log.info("Skipping test, private fixtures not enabled.")
            return

        with self.assertRaises(FixtureNotFoundError):
            private_fixture("not-a-real-fixture.txt")
