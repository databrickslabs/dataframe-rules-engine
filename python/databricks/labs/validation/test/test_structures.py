import unittest

from databricks.labs.validation.structures import MinMaxRuleDef, Bounds
from databricks.labs.validation.test.local_spark_singleton import SparkSingleton

import pyspark.sql.functions as F


class TestStructures(unittest.TestCase):

    def setup(self):
        self.spark = SparkSingleton.get_instance()

    def test_get_returns(self):
        self.setup()

        # Test Bounds
        sku_price_bounds = Bounds(1.0, 1000.0)
        assert (sku_price_bounds.lower == 1.0)
        assert (sku_price_bounds.upper == 1000.0)
        assert (not sku_price_bounds.lower_inclusive)
        assert (not sku_price_bounds.upper_inclusive)

        # Test MinMax Definitions
        min_max_no_agg = MinMaxRuleDef("valid_sku_prices", F.col("sku_price"), bounds=sku_price_bounds)
        assert min_max_no_agg.rule_name == "valid_sku_prices", "Invalid rule name for MinMax definition."
        assert (min_max_no_agg.bounds.lower == 1.0)
        assert (min_max_no_agg.bounds.upper == 1000.0)

        min_max_w_agg = MinMaxRuleDef("valid_sku_prices_agg", F.col("sku_price"), bounds=sku_price_bounds,
                                      by=[F.col("store_id"), F.col("product_id")])
        assert min_max_w_agg.rule_name == "valid_sku_prices_agg", "Invalid rule name for MinMax definition!"
        assert (min_max_w_agg.bounds.lower == 1.0)
        assert (min_max_w_agg.bounds.upper == 1000.0)

        # Clean up on aisle 2!
        self.tear_down()

    def tear_down(self):
        self.spark.stop()
