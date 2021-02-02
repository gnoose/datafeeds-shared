import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import _UtilityName_Transformer


class TestUrjanet_UtilityName_Transformer(test_util.UrjaCsvFixtureTest):
    def test_transformer(self):
        self.verify_transform(_UtilityName_Transformer(), "_UtilityId_")


if __name__ == "__main__":
    unittest.main()
