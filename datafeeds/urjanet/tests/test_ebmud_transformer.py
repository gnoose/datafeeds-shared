import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import EbmudTransformer


class TestUrjanetEbmudTransformer(test_util.UrjaCsvFixtureTest):
    def test_transformer(self):
        self.verify_transform(EbmudTransformer(), "ebmud")


if __name__ == "__main__":
    unittest.main()
