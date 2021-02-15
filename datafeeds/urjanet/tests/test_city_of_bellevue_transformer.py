import unittest

import datafeeds.urjanet.tests.util as test_util
from datafeeds.urjanet.transformer import CityOfBellevueTransformer


class TestUrjanetCityOfBellevueTransformer(test_util.UrjaCsvFixtureTest):
    def test_transformer(self):
        self.verify_transform(CityOfBellevueTransformer(), "city-of-bellevue")


if __name__ == "__main__":
    unittest.main()
