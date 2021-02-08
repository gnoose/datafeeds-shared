import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import ContraCostaWaterTransformer


class TestUrjanetContraCostaWaterTransformer(test_util.UrjaCsvFixtureTest):
    def test_transformer(self):
        self.verify_transform(ContraCostaWaterTransformer(), "contra-costa-water")


if __name__ == "__main__":
    unittest.main()
