import unittest

from databricks.labs.validation.utils.helpers import Helpers
from databricks.labs.validation.test.local_spark_singleton import SparkSingleton


class TestHelpers(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSingleton.get_instance()

    def test_convert_string_lov(self):
        valid_string = ["store_a", "store_b", "store_c"]
        j_valid_string = Helpers.to_java_array(valid_string, self.spark._sc)
        assert(j_valid_string[0] == "store_a")
        assert(j_valid_string[1] == "store_b")
        assert(j_valid_string[2] == "store_c")

    def test_convert_int_lov(self):
        valid_ints = [1, 2, 3]
        j_valid_ints = Helpers.to_java_array(valid_ints, self.spark._sc)
        assert(j_valid_ints[0] == 1)
        assert(j_valid_ints[1] == 2)
        assert(j_valid_ints[2] == 3)

    def tearDown(self):
        self.spark.stop()
