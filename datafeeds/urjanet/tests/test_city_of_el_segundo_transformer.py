import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import CityOfElSegundoTransformer


class TestUrjanetCityOfElSegundoTransformer(test_util.UrjaCsvFixtureTest):
    def test_transformer(self):
        self.verify_transform(CityOfElSegundoTransformer(), "city-of-el-segundo")


if __name__ == "__main__":
    unittest.main()
